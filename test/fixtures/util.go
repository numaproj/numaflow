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
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	flowpkg "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var OutputRegexp = func(rx string) func(t *testing.T, output string, err error) {
	return func(t *testing.T, output string, err error) {
		t.Helper()
		if assert.NoError(t, err, output) {
			assert.Regexp(t, rx, output)
		}
	}
}

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

func WaitForVertexPodRunning(kubeClient kubernetes.Interface, vertexClient flowpkg.VertexInterface, namespace, pipelineName, vertexName string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipelineName, dfv1.KeyVertexName, vertexName)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout after %v waiting for vertex pod running", timeout)
		default:
		}
		vertexList, err := vertexClient.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			return fmt.Errorf("error getting vertex list: %w", err)
		}
		ok := len(vertexList.Items) == 1
		podList, err := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
		if err != nil {
			return fmt.Errorf("error getting vertex pod name: %w", err)
		}
		ok = ok && len(podList.Items) > 0 && len(podList.Items) == int(*vertexList.Items[0].Spec.Replicas) // pod number should equal to desired replicas
		for _, p := range podList.Items {
			ok = ok && p.Status.Phase == corev1.PodRunning
		}
		if ok {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
}

func VertexPodLogNotContains(ctx context.Context, kubeClient kubernetes.Interface, namespace, pipelineName, vertexName, regex string, opts ...PodLogCheckOption) (bool, error) {
	labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipelineName, dfv1.KeyVertexName, vertexName)
	podList, err := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
	if err != nil {
		return false, fmt.Errorf("error getting vertex pods: %w", err)
	}
	return PodsLogNotContains(ctx, kubeClient, namespace, regex, podList, opts...), nil
}

func PodsLogNotContains(ctx context.Context, kubeClient kubernetes.Interface, namespace, regex string, podList *corev1.PodList, opts ...PodLogCheckOption) bool {
	o := defaultPodLogCheckOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	cctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	errChan := make(chan error)
	resultChan := make(chan bool)
	for _, p := range podList.Items {
		go func(podName string) {
			fmt.Printf("Watching POD: %s\n", podName)
			if err := podLogContains(cctx, kubeClient, namespace, podName, o.container, regex, resultChan, errChan); err != nil {
				errChan <- err
				return
			}
		}(p.Name)
	}
	for {
		select {
		case <-cctx.Done():
			return true // Consider timeout as not containing
		case result := <-resultChan:
			if result {
				return false
			}
		case err := <-errChan:
			fmt.Printf("error: %v", err)
		}
	}
}

func VertexPodLogContains(ctx context.Context, kubeClient kubernetes.Interface, namespace, pipelineName, vertexName, regex string, opts ...PodLogCheckOption) (bool, error) {
	labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipelineName, dfv1.KeyVertexName, vertexName)
	podList, err := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
	if err != nil {
		return false, fmt.Errorf("error getting vertex pods: %w", err)
	}
	return PodsLogContains(ctx, kubeClient, namespace, regex, podList, opts...), nil
}

func PodsLogContains(ctx context.Context, kubeClient kubernetes.Interface, namespace, regex string, podList *corev1.PodList, opts ...PodLogCheckOption) bool {
	o := defaultPodLogCheckOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	cctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	errChan := make(chan error)
	resultChan := make(chan bool)
	for _, p := range podList.Items {
		go func(podName string) {
			fmt.Printf("Watching POD: %s\n", podName)
			if err := podLogContains(cctx, kubeClient, namespace, podName, o.container, regex, resultChan, errChan); err != nil {
				errChan <- err
				return
			}
		}(p.Name)
	}

	matchTimes := 0
	for {
		select {
		case <-cctx.Done():
			return false // Consider timeout as false
		case result := <-resultChan:
			if result {
				if o.count < 0 {
					return true
				} else {
					matchTimes++
					if matchTimes >= o.count {
						return true
					}
				}
			}
		case err := <-errChan:
			fmt.Printf("error: %v", err)
		}
	}
}

func podLogContains(ctx context.Context, client kubernetes.Interface, namespace, podName, containerName, regex string, result chan bool, errs chan error) error {
	stream, err := client.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{Follow: true, Container: containerName}).Stream(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	exp, err := regexp.Compile(regex)
	if err != nil {
		return err
	}

	s := bufio.NewScanner(stream)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if !s.Scan() {
				return s.Err()
			}
			data := s.Bytes()
			fmt.Println(string(data))
			if exp.Match(data) {
				result <- true
			}
		}
	}
}

type podLogCheckOptions struct {
	container string
	timeout   time.Duration
	count     int
}

func defaultPodLogCheckOptions() *podLogCheckOptions {
	return &podLogCheckOptions{
		container: "",
		timeout:   defaultTimeout,
		count:     -1,
	}
}

type PodLogCheckOption func(*podLogCheckOptions)

func PodLogCheckOptionWithTimeout(t time.Duration) PodLogCheckOption {
	return func(o *podLogCheckOptions) {
		o.timeout = t
	}
}

func PodLogCheckOptionWithCount(c int) PodLogCheckOption {
	return func(o *podLogCheckOptions) {
		o.count = c
	}
}

func PodLogCheckOptionWithContainer(c string) PodLogCheckOption {
	return func(o *podLogCheckOptions) {
		o.container = c
	}
}
