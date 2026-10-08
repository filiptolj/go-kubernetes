package apiserver

import (
	"errors"
	"fmt"
	"path"
	"regexp"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/cron"
)

// The checks below run in the API server, before anything is stored, so a
// mistake in a file is reported to whoever applied it straight away, instead
// of failing later inside a kubelet or controller.

// validatePodSpec checks the containers, volumes and restart policy of a pod
// or pod template. allowed lists the restart policies this kind of object may use.
func validatePodSpec(containers []api.Container, volumes []api.Volume, policy api.RestartPolicy, allowed ...api.RestartPolicy) error {
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
		case v.ConfigMap != nil && v.ConfigMap.Name == "", v.Secret != nil && v.Secret.Name == "":
			return fmt.Errorf("volume %q needs the name of its configMap or secret", v.Name)
		}
	}

	containerNames := make(map[string]bool)
	for _, c := range containers {
		if c.Name == "" || containerNames[c.Name] {
			return fmt.Errorf("every container needs a name of its own (%q)", c.Name)
		}
		containerNames[c.Name] = true

		if c.Port < 0 || c.Port > 65535 {
			return fmt.Errorf("container %q: port must be between 1 and 65535", c.Name)
		}
		if (c.LivenessProbe != nil || c.ReadinessProbe != nil) && c.Port == 0 {
			return fmt.Errorf("container %q: probes check the container's port, so it needs a port", c.Name)
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
	return nil
}

// prepareTemplate gives a pod template the default restart policy, Always,
// and checks it. Templates of ReplicaSets, Deployments, DaemonSets and
// StatefulSets must keep their pods running, so Always is the only choice.
func prepareTemplate(t *api.PodTemplate) error {
	if t.RestartPolicy == "" {
		t.RestartPolicy = api.RestartAlways
	}
	err := validatePodSpec(t.Containers, t.Volumes, t.RestartPolicy, api.RestartAlways)
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
	err := validatePodSpec(spec.Template.Containers, spec.Template.Volumes, spec.Template.RestartPolicy,
		api.RestartOnFailure, api.RestartNever)
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
	return prepareJobSpec(&c.JobTemplate)
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

func prepareSecret(s *api.Secret) error {
	if s.Data == nil {
		s.Data = map[string]string{}
	}
	return checkData(s.Data)
}
