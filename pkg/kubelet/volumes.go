package kubelet

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/cri"
)

// containerSetup is what one container starts with, worked out from its pod.
type containerSetup struct {
	env    []string // "NAME=value"
	mounts []cri.Mount
}

// prepare creates the pod's volumes on this node, and works out each
// container's environment and mounts. It fails if a ConfigMap or Secret the
// pod uses doesn't exist.
func (k *Kubelet) prepare(pod api.Pod) (map[string]containerSetup, error) {
	values := &lookup{k: k, namespace: pod.Namespace}

	// Make every volume, and note where it is on this node.
	hostPath := make(map[string]string) // by volume name
	readOnly := make(map[string]bool)
	for _, v := range pod.Volumes {
		dir := filepath.Join(k.volumeDir(pod), v.Name)
		switch {
		case v.HostPath != nil:
			hostPath[v.Name] = v.HostPath.Path

		case v.EmptyDir != nil:
			err := os.MkdirAll(dir, 0o777)
			if err == nil {
				// MkdirAll's permissions are reduced by the umask (usually to
				// 0755), so set them again: containers may run as another user
				// and still need to write here.
				err = os.Chmod(dir, 0o777)
			}
			if err != nil {
				return nil, fmt.Errorf("volume %q: %w", v.Name, err)
			}
			hostPath[v.Name] = dir

		case v.ConfigMap != nil, v.Secret != nil:
			data, err := values.data(v.ConfigMap, v.Secret)
			if err != nil {
				return nil, fmt.Errorf("volume %q: %w", v.Name, err)
			}
			err = writeFiles(dir, data)
			if err != nil {
				return nil, fmt.Errorf("volume %q: %w", v.Name, err)
			}
			hostPath[v.Name] = dir
			readOnly[v.Name] = true
		}
	}

	setups := make(map[string]containerSetup)
	for _, c := range pod.Containers {
		// Every container learns its pod's and node's names, as Kubernetes'
		// "downward API" can provide.
		env := []string{
			"POD_NAME=" + pod.Name,
			"POD_NAMESPACE=" + pod.Namespace,
			"NODE_NAME=" + k.cfg.NodeName,
		}
		for _, e := range c.Env {
			value := e.Value
			if e.ValueFrom != nil {
				var err error
				value, err = values.env(e.ValueFrom)
				if err != nil {
					return nil, fmt.Errorf("container %q, variable %s: %w", c.Name, e.Name, err)
				}
			}
			if strings.ContainsAny(value, "\r\n") {
				return nil, fmt.Errorf("container %q, variable %s: values can't contain line breaks", c.Name, e.Name)
			}
			env = append(env, e.Name+"="+value)
		}

		var mounts []cri.Mount
		for _, m := range c.VolumeMounts {
			mounts = append(mounts, cri.Mount{
				HostPath:      hostPath[m.Name],
				ContainerPath: m.MountPath,
				ReadOnly:      m.ReadOnly || readOnly[m.Name],
			})
		}

		setups[c.Name] = containerSetup{env: env, mounts: mounts}
	}
	return setups, nil
}

// volumeDir is where the volumes the kubelet creates for a pod live.
func (k *Kubelet) volumeDir(pod api.Pod) string {
	return filepath.Join(k.cfg.VolumeDir, pod.Namespace, pod.Name)
}

// removeVolumes deletes the volumes the kubelet created for a pod. Host
// paths are the node's own folders, outside volumeDir, and are left alone.
func (k *Kubelet) removeVolumes(pod api.Pod) {
	err := os.RemoveAll(k.volumeDir(pod))
	if err != nil {
		k.events.Warning("Pod", pod.Namespace, pod.Name, "FailedCleanup", "could not remove volumes: %v", err)
	}
}

// writeFiles fills dir with one file per key of data, replacing whatever was there.
func writeFiles(dir string, data map[string]string) error {
	err := os.RemoveAll(dir)
	if err == nil {
		err = os.MkdirAll(dir, 0o755)
	}
	for key, value := range data {
		if err != nil {
			break
		}
		err = os.WriteFile(filepath.Join(dir, key), []byte(value), 0o644)
	}
	return err
}

// lookup fetches the ConfigMaps and Secrets one pod uses, each only once.
type lookup struct {
	k          *Kubelet
	namespace  string
	configMaps map[string]map[string]string // data, by name
	secrets    map[string]map[string]string
}

// data returns the data of a ConfigMap (if cm is set) or a Secret.
func (l *lookup) data(cm, secret *api.ObjectRef) (map[string]string, error) {
	if cm != nil {
		return l.configMap(cm.Name)
	}
	return l.secret(secret.Name)
}

// env returns the value an environment variable refers to.
func (l *lookup) env(src *api.EnvSource) (string, error) {
	var data map[string]string
	var ref *api.KeyRef
	var err error
	if src.ConfigMapKeyRef != nil {
		ref = src.ConfigMapKeyRef
		data, err = l.configMap(ref.Name)
	} else {
		ref = src.SecretKeyRef
		data, err = l.secret(ref.Name)
	}
	if err != nil {
		return "", err
	}

	value, ok := data[ref.Key]
	if !ok {
		return "", fmt.Errorf("%q has no key %q", ref.Name, ref.Key)
	}
	return value, nil
}

func (l *lookup) configMap(name string) (map[string]string, error) {
	if data, ok := l.configMaps[name]; ok {
		return data, nil
	}
	cm, err := l.k.client.ConfigMaps().Get(l.namespace, name)
	if err != nil {
		return nil, err
	}
	if l.configMaps == nil {
		l.configMaps = make(map[string]map[string]string)
	}
	l.configMaps[name] = cm.Data
	return cm.Data, nil
}

func (l *lookup) secret(name string) (map[string]string, error) {
	if data, ok := l.secrets[name]; ok {
		return data, nil
	}
	s, err := l.k.client.Secrets().Get(l.namespace, name)
	if err != nil {
		return nil, err
	}
	if l.secrets == nil {
		l.secrets = make(map[string]map[string]string)
	}
	l.secrets[name] = s.Data
	return s.Data, nil
}
