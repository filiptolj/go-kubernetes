package apiserver

import (
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/cron"
)

// The checks below run in the API server, before anything is stored, so a
// mistake in a file is reported to whoever applied it straight away, instead
// of failing later inside a kubelet or controller.

// validatePodSpec checks the containers, volumes and restart policy of a pod
// or pod template, and fills in defaults. allowed lists the restart policies
// this kind of object may use.
func validatePodSpec(spec *api.PodSpec, allowed ...api.RestartPolicy) error {
	containers, volumes, policy := spec.Containers, spec.Volumes, spec.RestartPolicy
	if len(containers) == 0 {
		return errors.New("needs at least one container")
	}

	ok := false
	for _, p := range allowed {
		ok = ok || p == policy
	}
	if !ok {
		return fmt.Errorf("restartPolicy %q isn't allowed here; use one of %v", policy, allowed)
	}

	volumeNames := make(map[string]bool)
	for _, v := range volumes {
		if v.Name == "" || volumeNames[v.Name] {
			return fmt.Errorf("every volume needs a name of its own (%q)", v.Name)
		}
		volumeNames[v.Name] = true

		sources := 0
		for _, set := range []bool{v.EmptyDir != nil, v.HostPath != nil, v.ConfigMap != nil, v.Secret != nil} {
			if set {
				sources++
			}
		}
		switch {
		case sources != 1:
			return fmt.Errorf("volume %q needs exactly one of emptyDir, hostPath, configMap or secret", v.Name)
		case v.HostPath != nil && !path.IsAbs(v.HostPath.Path):
			return fmt.Errorf("volume %q: hostPath must be an absolute path", v.Name)
		case v.ConfigMap != nil && v.ConfigMap.Name == "", v.Secret != nil && v.Secret.SecretName == "":
			return fmt.Errorf("volume %q needs the name of its configMap or secret", v.Name)
		}
	}

	for _, c := range spec.InitContainers {
		if c.LivenessProbe != nil || c.ReadinessProbe != nil {
			return fmt.Errorf("init container %q: init containers run to completion, so they can't have probes", c.Name)
		}
	}

	// Init containers are checked like the others: they share the names,
	// the volumes, and the rules.
	all := slices.Concat(spec.InitContainers, containers)
	containerNames := make(map[string]bool)
	for _, c := range all {
		if c.Name == "" || containerNames[c.Name] {
			return fmt.Errorf("every container needs a name of its own (%q)", c.Name)
		}
		containerNames[c.Name] = true

		for _, p := range c.Ports {
			if p.ContainerPort < 1 || p.ContainerPort > 65535 {
				return fmt.Errorf("container %q: containerPort must be between 1 and 65535", c.Name)
			}
		}
		for _, p := range []*api.Probe{c.LivenessProbe, c.ReadinessProbe} {
			switch {
			case p == nil:
			case p.Exec != nil && p.HTTPGet != nil:
				return fmt.Errorf("container %q: a probe uses either exec or httpGet, not both", c.Name)
			case p.Exec != nil && len(p.Exec.Command) == 0:
				return fmt.Errorf("container %q: an exec probe needs a command", c.Name)
			case p.Exec == nil && c.Port() == 0:
				return fmt.Errorf("container %q: probes without exec check the container's port, so it needs a port", c.Name)
			}
		}

		for _, e := range c.Env {
			if e.Name == "" {
				return fmt.Errorf("container %q: every environment variable needs a name", c.Name)
			}
			if e.ValueFrom != nil {
				cm, sec := e.ValueFrom.ConfigMapKeyRef, e.ValueFrom.SecretKeyRef
				if (cm == nil) == (sec == nil) {
					return fmt.Errorf("container %q, variable %s: valueFrom needs exactly one of configMapKeyRef or secretKeyRef", c.Name, e.Name)
				}
				ref := cm
				if ref == nil {
					ref = sec
				}
				if ref.Name == "" || ref.Key == "" {
					return fmt.Errorf("container %q, variable %s: the reference needs a name and a key", c.Name, e.Name)
				}
			}
		}

		for _, m := range c.VolumeMounts {
			if !volumeNames[m.Name] {
				return fmt.Errorf("container %q mounts volume %q, but the pod has no volume with that name", c.Name, m.Name)
			}
			if !path.IsAbs(m.MountPath) {
				return fmt.Errorf("container %q: mountPath %q must be an absolute path", c.Name, m.MountPath)
			}
		}
	}

	// By index, not with a copy: prepareResources fills in defaults.
	for _, list := range [][]api.Container{spec.InitContainers, containers} {
		for i := range list {
			err := prepareResources(&list[i].Resources)
			if err != nil {
				return fmt.Errorf("container %q: %w", list[i].Name, err)
			}
		}
	}
	return nil
}

// prepareResources checks a container's requests and limits. A resource
// with a limit but no request gets a request equal to its limit, as in
// Kubernetes: what a container may use is then also what it is counted for.
func prepareResources(r *api.ResourceRequirements) error {
	for _, list := range []api.ResourceList{r.Requests, r.Limits} {
		for name, value := range list {
			var err error
			switch name {
			case api.ResourceCPU:
				_, err = api.ParseCPU(value)
			case api.ResourceMemory:
				_, err = api.ParseMemory(value)
			default:
				err = fmt.Errorf("unknown resource %q: use cpu or memory", name)
			}
			if err != nil {
				return err
			}
		}
	}

	for name, limit := range r.Limits {
		if _, ok := r.Requests[name]; !ok {
			if r.Requests == nil {
				r.Requests = make(api.ResourceList)
			}
			r.Requests[name] = limit
		}
	}

	requests, limits := api.ParseResources(r.Requests), api.ParseResources(r.Limits)
	if _, ok := r.Limits[api.ResourceCPU]; ok && requests.CPU > limits.CPU {
		return fmt.Errorf("cpu request %s is more than its limit %s", r.Requests[api.ResourceCPU], r.Limits[api.ResourceCPU])
	}
	if _, ok := r.Limits[api.ResourceMemory]; ok && requests.Memory > limits.Memory {
		return fmt.Errorf("memory request %s is more than its limit %s", r.Requests[api.ResourceMemory], r.Limits[api.ResourceMemory])
	}
	return nil
}

// prepareTemplate gives a pod template the default restart policy, Always,
// and checks it. Templates of ReplicaSets, Deployments, DaemonSets and
// StatefulSets must keep their pods running, so Always is the only choice.
func prepareTemplate(t *api.PodTemplateSpec) error {
	if t.RestartPolicy == "" {
		t.RestartPolicy = api.RestartAlways
	}
	err := validatePodSpec(&t.PodSpec, api.RestartAlways)
	if err != nil {
		return fmt.Errorf("template: %w", err)
	}
	return nil
}

// prepareJobSpec fills in a JobSpec's defaults and checks it.
func prepareJobSpec(spec *api.JobSpec) error {
	if spec.Completions == 0 {
		spec.Completions = 1
	}
	if spec.Parallelism == 0 {
		spec.Parallelism = 1
	}
	if spec.BackoffLimit == nil {
		six := 6
		spec.BackoffLimit = &six
	}
	if spec.Completions < 0 || spec.Parallelism < 0 || *spec.BackoffLimit < 0 {
		return errors.New("completions, parallelism and backoffLimit can't be negative")
	}

	if spec.Template.RestartPolicy == "" {
		spec.Template.RestartPolicy = api.RestartNever
	}
	err := validatePodSpec(&spec.Template.PodSpec, api.RestartOnFailure, api.RestartNever)
	if err != nil {
		return fmt.Errorf("template: %w", err)
	}
	return nil
}

func prepareJob(j *api.Job) error {
	return prepareJobSpec(&j.JobSpec)
}

func prepareCronJob(c *api.CronJob) error {
	_, err := cron.Parse(c.Schedule)
	if err != nil {
		return err
	}
	return prepareJobSpec(&c.JobTemplate.Spec)
}

func prepareDaemonSet(d *api.DaemonSet) error {
	return prepareTemplate(&d.Template)
}

func prepareStatefulSet(s *api.StatefulSet) error {
	if s.Replicas < 0 {
		return errors.New("replicas can't be negative")
	}
	return prepareTemplate(&s.Template)
}

// validKey matches keys that can be file names in a volume and environment
// variable values: letters, digits, '-', '_' and '.'.
var validKey = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// checkData checks the keys of a ConfigMap or Secret.
func checkData(data map[string]string) error {
	for key := range data {
		if !validKey.MatchString(key) || key == "." || key == ".." {
			return fmt.Errorf("key %q: keys may only use letters, digits, '-', '_' and '.'", key)
		}
	}
	return nil
}

func prepareConfigMap(c *api.ConfigMap) error {
	if c.Data == nil {
		c.Data = map[string]string{}
	}
	return checkData(c.Data)
}

// prepareSecret moves stringData, plain text written by hand, into data.
func prepareSecret(s *api.Secret) error {
	if s.Data == nil {
		s.Data = map[string][]byte{}
	}
	for key, value := range s.StringData {
		s.Data[key] = []byte(value)
	}
	s.StringData = nil

	for key := range s.Data {
		if !validKey.MatchString(key) || key == "." || key == ".." {
			return fmt.Errorf("key %q: keys may only use letters, digits, '-', '_' and '.'", key)
		}
	}
	return nil
}

// prepareService checks a Service's selector and ports.
func prepareService(svc *api.Service) error {
	if len(svc.Selector) == 0 {
		return errors.New("selector is required, so the service can find its pods")
	}
	if len(svc.Ports) == 0 {
		return errors.New("a service needs at least one port")
	}
	for _, p := range svc.Ports {
		if p.Port < 1 || p.Port > 65535 || p.TargetPort < 0 || p.TargetPort > 65535 {
			return errors.New("ports must be between 1 and 65535")
		}
	}
	return nil
}

// setKind fills in an object's apiVersion and kind, and refuses an object
// that says it is of another kind, such as a Pod sent to the jobs URL.
func setKind(t *api.TypeMeta, kind string) error {
	if t.Kind != "" && t.Kind != kind {
		return fmt.Errorf("expected kind %q, got %q", kind, t.Kind)
	}
	if t.APIVersion != "" && t.APIVersion != api.APIVersion(kind) {
		return fmt.Errorf("%s belongs to apiVersion %q, not %q", kind, api.APIVersion(kind), t.APIVersion)
	}
	*t = api.TypeMetaFor(kind)
	return nil
}
