package fixtures

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	flowpkg "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func Exec(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Env = os.Environ()
	println(cmd.String())
	output, err := runWithTimeout(cmd)
	// Command completed before timeout. Print output and error if it exists.
	if err != nil {
		_, _ = fmt.Fprint(os.Stderr, err)
	}
	for _, s := range strings.Split(output, "\n") {
		println(s)
	}
	return output, err
}

func runWithTimeout(cmd *exec.Cmd) (string, error) {
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Start()
	if err != nil {
		return "", err
	}
	done := make(chan error)
	go func() { done <- cmd.Wait() }()
	timeout := time.After(60 * time.Second)
	select {
	case <-timeout:
		_ = cmd.Process.Kill()
		return buf.String(), fmt.Errorf("timeout")
	case err := <-done:
		return buf.String(), err
	}
}

func WaitForISBSvcReady(ctx context.Context, isbSvcClient flowpkg.InterStepBufferServiceInterface, isbSvcName string, timeout time.Duration) error {
	fieldSelector := "metadata.name=" + isbSvcName
	opts := metav1.ListOptions{FieldSelector: fieldSelector}
	watch, err := isbSvcClient.Watch(ctx, opts)
	if err != nil {
		return err
	}
	defer watch.Stop()
	timeoutCh := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		timeoutCh <- true
	}()
	for {
		select {
		case event := <-watch.ResultChan():
			i, ok := event.Object.(*dfv1.InterStepBufferService)
			if ok {
				if i.Status.IsReady() {
					return nil
				}
			} else {
				return fmt.Errorf("not isb svc")
			}
		case <-timeoutCh:
			return fmt.Errorf("timeout after %v waiting for ISB svc ready", timeout)
		}
	}
}

func WaitForISBSvcStatefulSetReady(ctx context.Context, kubeClient kubernetes.Interface, namespace, isbSvcName string, timeout time.Duration) error {
	labelSelector := fmt.Sprintf("%s=isbsvc-controller,%s=%s", dfv1.KeyManagedBy, dfv1.KeyISBSvcName, isbSvcName)
	opts := metav1.ListOptions{LabelSelector: labelSelector}
	watch, err := kubeClient.AppsV1().StatefulSets(namespace).Watch(ctx, opts)
	if err != nil {
		return err
	}
	defer watch.Stop()
	timeoutCh := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		timeoutCh <- true
	}()

statefulSetWatch:
	for {
		select {
		case event := <-watch.ResultChan():
			ss, ok := event.Object.(*appsv1.StatefulSet)
			if ok {
				if ss.Status.Replicas == ss.Status.ReadyReplicas {
					break statefulSetWatch
				}
			} else {
				return fmt.Errorf("not statefulset")
			}
		case <-timeoutCh:
			return fmt.Errorf("timeout after %v waiting for ISB svc StatefulSet ready", timeout)
		}
	}

	// POD
	podWatch, err := kubeClient.CoreV1().Pods(namespace).Watch(ctx, opts)
	if err != nil {
		return err
	}
	defer podWatch.Stop()
	podTimeoutCh := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		podTimeoutCh <- true
	}()

	podNames := make(map[string]bool)
	for {
		if len(podNames) == 3 {
			// defaults to 3 Pods
			return nil
		}
		select {
		case event := <-podWatch.ResultChan():
			p, ok := event.Object.(*corev1.Pod)
			if ok {
				if p.Status.Phase == corev1.PodRunning {
					podReady := true
					for _, cs := range p.Status.ContainerStatuses {
						if !cs.Ready {
							podReady = false
						}
					}
					if podReady {
						if _, existing := podNames[p.GetName()]; !existing {
							podNames[p.GetName()] = true
						}
					}
				}
			} else {
				return fmt.Errorf("not pod")
			}
		case <-podTimeoutCh:
			return fmt.Errorf("timeout after %v waiting for ISB svc Pod ready", timeout)
		}
	}
}

func WaitForPipelineRunning(ctx context.Context, pipelineClient flowpkg.PipelineInterface, pipelineName string, timeout time.Duration) error {
	fieldSelector := "metadata.name=" + pipelineName
	opts := metav1.ListOptions{FieldSelector: fieldSelector}
	watch, err := pipelineClient.Watch(ctx, opts)
	if err != nil {
		return err
	}
	defer watch.Stop()
	timeoutCh := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		timeoutCh <- true
	}()
	for {
		select {
		case event := <-watch.ResultChan():
			i, ok := event.Object.(*dfv1.Pipeline)
			if ok {
				if i.Status.Phase == dfv1.PipelinePhaseRunning {
					return nil
				}
			} else {
				return fmt.Errorf("not pipeline")
			}
		case <-timeoutCh:
			return fmt.Errorf("timeout after %v waiting for Pipeline running", timeout)
		}
	}
}

func WaitForVertexPodRunning(kubeClient kubernetes.Interface, namespace, pipelineName, vertexName string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipelineName, dfv1.KeyVertexName, vertexName)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout after %v waiting for vertex pod running", timeout)
		default:
		}
		podList, err := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
		if err != nil {
			return fmt.Errorf("error getting vertex pod name: %w", err)
		}
		ok := len(podList.Items) > 0
		for _, p := range podList.Items {
			ok = ok && p.Status.Phase == corev1.PodRunning
		}
		if ok {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
}

func VertexPodLogContains(ctx context.Context, kubeClient kubernetes.Interface, namespace, pipelineName, vertexName, containerName, regex string, timeout time.Duration) (bool, error) {
	labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipelineName, dfv1.KeyVertexName, vertexName)
	podList, err := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
	if err != nil {
		return false, fmt.Errorf("error getting vertex pod name: %w", err)
	}
	return PodsLogContains(ctx, kubeClient, namespace, regex, podList, containerName, timeout), nil
}

func PodsLogContains(ctx context.Context, kubeClient kubernetes.Interface, namespace, regex string, podList *corev1.PodList, containerName string, timeout time.Duration) bool {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	errChan := make(chan error)
	resultChan := make(chan bool)
	for _, p := range podList.Items {
		go func(podName string) {
			fmt.Printf("Watching POD: %s\n", podName)
			contains, err := podLogContains(cctx, kubeClient, namespace, podName, containerName, regex)
			if err != nil {
				errChan <- err
				return
			}
			if contains {
				resultChan <- true
			}
		}(p.Name)
	}

	for {
		select {
		case <-cctx.Done():
			return false
		case result := <-resultChan:
			if result {
				return true
			}
		case err := <-errChan:
			fmt.Printf("error: %v", err)
		}
	}
}

func podLogContains(ctx context.Context, client kubernetes.Interface, namespace, podName, containerName, regex string) (bool, error) {
	stream, err := client.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{Follow: true, Container: containerName}).Stream(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = stream.Close() }()

	exp, err := regexp.Compile(regex)
	if err != nil {
		return false, err
	}

	s := bufio.NewScanner(stream)
	for {
		select {
		case <-ctx.Done():
			return false, nil
		default:
			if !s.Scan() {
				return false, s.Err()
			}
			data := s.Bytes()
			fmt.Println(string(data))
			if exp.Match(data) {
				return true, nil
			}
		}
	}
}
