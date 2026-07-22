package workflow_test

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/karupanerura/google-cloud-workflow-emulator/internal/workflow"
)

func mustParseWorkflowYAML(t *testing.T, source string) workflow.WorkflowRoot {
	t.Helper()

	root, err := workflow.ParseWorkflowYAML(strings.NewReader(source))
	if err != nil {
		t.Fatalf("ParseWorkflowYAML: %v", err)
	}
	return root
}

func executeWorkflowYAML(t *testing.T, source string) (any, error) {
	t.Helper()

	return mustParseWorkflowYAML(t, source).Execute(nil)
}

func TestParallelBranchesSharedWriteback(t *testing.T) {
	ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - counter: 0
    - par:
        parallel:
          shared: [counter]
          branches:
            - branch1:
                steps:
                  - inc1:
                      assign:
                        - counter: ${counter + 1}
            - branch2:
                steps:
                  - inc2:
                      assign:
                        - counter: ${counter + 10}
            - branch3:
                steps:
                  - inc3:
                      assign:
                        - counter: ${counter + 100}
    - done:
        return: ${counter}
`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ret != int64(111) {
		t.Errorf("should return 111 but got %v (%T)", ret, ret)
	}
}

func TestParallelSharedDuplicateAssignInOneStep(t *testing.T) {
	ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - counter: 0
    - par:
        parallel:
          shared: [counter]
          branches:
            - branch1:
                steps:
                  - inc1:
                      assign:
                        - counter: ${counter + 1}
                        - counter: ${counter + 10}
                        - counter: ${counter + 100}
            - branch2:
                steps:
                  - inc2:
                      assign:
                        - counter: ${counter + 1}
                        - counter: ${counter + 10}
                        - counter: ${counter + 100}
    - done:
        return: ${counter}
`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ret != int64(222) {
		t.Errorf("should return 222 but got %v (%T)", ret, ret)
	}
}

func TestParallelBranchesSharedStress(t *testing.T) {
	var branches strings.Builder
	for i := 1; i <= 8; i++ {
		branches.WriteString(fmt.Sprintf("            - branch%d:\n                steps:\n", i))
		for j := 1; j <= 5; j++ {
			branches.WriteString(fmt.Sprintf("                  - inc%d_%d:\n                      assign:\n                        - counter: ${counter + 1}\n", i, j))
		}
	}

	root := mustParseWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - counter: 0
    - par:
        parallel:
          shared: [counter]
          branches:
`+branches.String()+`
    - done:
        return: ${counter}
`)
	for i := 0; i < 20; i++ {
		ret, err := root.Execute(nil)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if ret != int64(40) {
			t.Errorf("should return 40 but got %v (%T)", ret, ret)
		}
	}
}

func TestParallelBranchAssignNonSharedFails(t *testing.T) {
	_, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - counter: 0
    - par:
        parallel:
          branches:
            - branch1:
                steps:
                  - inc1:
                      assign:
                        - counter: ${counter + 1}
    - done:
        return: ${counter}
`)
	if err == nil {
		t.Fatal("should fail to assign to a non-shared variable in a parallel branch")
	}
	if !strings.Contains(err.Error(), "cannot assign to non-shared variable") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParallelForSharedAggregate(t *testing.T) {
	ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - total: 0
    - par:
        parallel:
          shared: [total]
          for:
            value: v
            in: [1, 2, 3]
            steps:
              - add:
                  assign:
                    - total: ${total + v}
    - done:
        return: ${total}
`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ret != int64(6) {
		t.Errorf("should return 6 but got %v (%T)", ret, ret)
	}
}

func TestParallelCallResultNonSharedFails(t *testing.T) {
	_, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - outer: ""
    - par:
        parallel:
          branches:
            - branch1:
                steps:
                  - c1:
                      call: sys.get_env
                      args: ["NO_SUCH_ENV_VAR_FOR_TEST", "fallback"]
                      result: outer
    - done:
        return: ${outer}
`)
	if err == nil {
		t.Fatal("should fail to assign a call result to a non-shared variable in a parallel branch")
	}
	if !strings.Contains(err.Error(), "cannot assign call result to non-shared variable") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParallelBranchLocalVariable(t *testing.T) {
	t.Run("allowed inside a branch", func(t *testing.T) {
		ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - par:
        parallel:
          branches:
            - branch1:
                steps:
                  - local1:
                      assign:
                        - local: 1
                        - local: ${local + 1}
    - done:
        return: "ok"
`)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if ret != "ok" {
			t.Errorf("should return ok but got %v (%T)", ret, ret)
		}
	})

	t.Run("not visible after the parallel step", func(t *testing.T) {
		_, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - par:
        parallel:
          branches:
            - branch1:
                steps:
                  - local1:
                      assign:
                        - local: 1
    - done:
        return: ${local}
`)
		if err == nil {
			t.Fatal("branch local variable should not be visible after the parallel step")
		}
		if !strings.Contains(err.Error(), "not found symbol") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestParallelSharedUndefinedFails(t *testing.T) {
	_, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - par:
        parallel:
          shared: [nosuchvar]
          branches:
            - branch1:
                steps:
                  - nop1:
                      assign:
                        - local: 1
    - done:
        return: "ok"
`)
	if err == nil {
		t.Fatal("should fail to share an undefined variable")
	}
	if !strings.Contains(err.Error(), "is not defined") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParallelSharedMapFieldWrites(t *testing.T) {
	ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - m: {}
    - par:
        parallel:
          shared: [m]
          branches:
            - branch1:
                steps:
                  - set1:
                      assign:
                        - m.a: 1
            - branch2:
                steps:
                  - set2:
                      assign:
                        - m.b: 2
            - branch3:
                steps:
                  - set3:
                      assign:
                        - m.c: 3
    - done:
        return: ${m}
`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	expected := map[string]any{"a": int64(1), "b": int64(2), "c": int64(3)}
	if !reflect.DeepEqual(ret, expected) {
		t.Errorf("should return %v but got %v (%T)", expected, ret, ret)
	}
}

func TestParallelMutualAssignNoDeadlock(t *testing.T) {
	ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - a: 0
          - b: 0
    - par:
        parallel:
          shared: [a, b]
          branches:
            - branch1:
                steps:
                  - s1:
                      assign:
                        - a: ${b + 1}
                  - s2:
                      assign:
                        - a: ${b + 1}
                  - s3:
                      assign:
                        - a: ${b + 1}
            - branch2:
                steps:
                  - s1:
                      assign:
                        - b: ${a + 1}
                  - s2:
                      assign:
                        - b: ${a + 1}
                  - s3:
                      assign:
                        - b: ${a + 1}
    - done:
        return: "done"
`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ret != "done" {
		t.Errorf("should return done but got %v (%T)", ret, ret)
	}
}

func TestNestedParallelAdoption(t *testing.T) {
	t.Run("re-shared variable keeps updates", func(t *testing.T) {
		ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - counter: 0
    - outer:
        parallel:
          shared: [counter]
          branches:
            - outer1:
                steps:
                  - inner:
                      parallel:
                        shared: [counter]
                        branches:
                          - inner1:
                              steps:
                                - i1:
                                    assign:
                                      - counter: ${counter + 1}
                          - inner2:
                              steps:
                                - i2:
                                    assign:
                                      - counter: ${counter + 10}
            - outer2:
                steps:
                  - o2:
                      assign:
                        - counter: ${counter + 100}
    - done:
        return: ${counter}
`)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if ret != int64(111) {
			t.Errorf("should return 111 but got %v (%T)", ret, ret)
		}
	})

	t.Run("not re-shared variable is not writable", func(t *testing.T) {
		_, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - counter: 0
    - outer:
        parallel:
          shared: [counter]
          branches:
            - outer1:
                steps:
                  - inner:
                      parallel:
                        branches:
                          - inner1:
                              steps:
                                - i1:
                                    assign:
                                      - counter: ${counter + 1}
    - done:
        return: ${counter}
`)
		if err == nil {
			t.Fatal("should fail to assign to a variable not re-shared by the inner parallel step")
		}
		if !strings.Contains(err.Error(), "cannot assign to non-shared variable") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestConcurrencyLimit(t *testing.T) {
	t.Run("limits concurrent branches", func(t *testing.T) {
		// with concurrency_limit: 1 the branches must run one at a time, so the
		// overlap check can never observe current > 1
		ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - current: 0
    - par:
        parallel:
          shared: [current]
          concurrency_limit: 1
          branches:
            - branch1:
                steps:
                  - inc1:
                      assign:
                        - current: ${current + 1}
                  - chk1:
                      switch:
                        - condition: ${current > 1}
                          raise: "overlap detected"
                  - dec1:
                      assign:
                        - current: ${current - 1}
            - branch2:
                steps:
                  - inc2:
                      assign:
                        - current: ${current + 1}
                  - chk2:
                      switch:
                        - condition: ${current > 1}
                          raise: "overlap detected"
                  - dec2:
                      assign:
                        - current: ${current - 1}
            - branch3:
                steps:
                  - inc3:
                      assign:
                        - current: ${current + 1}
                  - chk3:
                      switch:
                        - condition: ${current > 1}
                          raise: "overlap detected"
                  - dec3:
                      assign:
                        - current: ${current - 1}
    - done:
        return: "ok"
`)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if ret != "ok" {
			t.Errorf("should return ok but got %v (%T)", ret, ret)
		}
	})

	for _, invalid := range []string{"0", "-1", "1.5"} {
		invalid := invalid
		t.Run("rejects "+invalid, func(t *testing.T) {
			_, err := workflow.ParseWorkflowYAML(strings.NewReader(`
main:
  params: [args]
  steps:
    - par:
        parallel:
          concurrency_limit: ` + invalid + `
          branches:
            - branch1:
                steps:
                  - nop1:
                      assign:
                        - local: 1
`))
			if err == nil {
				t.Fatalf("should fail to parse concurrency_limit: %s", invalid)
			}
			if !strings.Contains(err.Error(), "invalid concurrency_limit") {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestTryExceptNamedSteps(t *testing.T) {
	ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - guarded:
        try:
          steps:
            - prepare:
                assign:
                  - x: 1
            - boom:
                raise: "boom"
        except:
          as: e
          steps:
            - recovered:
                return: ${x}
    - done:
        return: "not reached"
`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ret != int64(1) {
		t.Errorf("should return 1 but got %v (%T)", ret, ret)
	}
}

func TestParallelExceptionPolicyContinueAll(t *testing.T) {
	// branch1 raises, but the other branches must run to completion and their
	// writes to the shared variable must persist (continueAll)
	ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - init:
        assign:
          - counter: 0
    - guarded:
        try:
          steps:
            - par:
                parallel:
                  shared: [counter]
                  branches:
                    - branch1:
                        steps:
                          - boom:
                              raise: "boom"
                    - branch2:
                        steps:
                          - inc2:
                              assign:
                                - counter: ${counter + 1}
                    - branch3:
                        steps:
                          - inc3:
                              assign:
                                - counter: ${counter + 1}
        except:
          as: e
          steps:
            - recovered:
                return: ${counter}
    - done:
        return: "not reached"
`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ret != int64(2) {
		t.Errorf("should return 2 but got %v (%T)", ret, ret)
	}
}

func TestExampleParallelBranchesYAML(t *testing.T) {
	f, err := os.Open("../../example/parallel_branches.yaml")
	if err != nil {
		t.Fatalf("os.Open: %v", err)
	}
	defer f.Close()

	root, err := workflow.ParseWorkflowYAML(f)
	if err != nil {
		t.Fatalf("ParseWorkflowYAML: %v", err)
	}

	ret, err := root.Execute(nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ret != int64(111) {
		t.Errorf("should return 111 but got %v (%T)", ret, ret)
	}
}

func TestReturnRejectedInParallel(t *testing.T) {
	t.Run("in a branch", func(t *testing.T) {
		_, err := workflow.ParseWorkflowYAML(strings.NewReader(`
main:
  params: [args]
  steps:
    - par:
        parallel:
          branches:
            - branch1:
                steps:
                  - r1:
                      return: "from-branch"
`))
		if err == nil {
			t.Fatal("should fail to compile a return step in a parallel branch")
		}
		if !strings.Contains(err.Error(), "return is not allowed in a parallel branch") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("nested in a switch in a branch", func(t *testing.T) {
		_, err := workflow.ParseWorkflowYAML(strings.NewReader(`
main:
  params: [args]
  steps:
    - init:
        assign:
          - flag: true
    - par:
        parallel:
          branches:
            - branch1:
                steps:
                  - s1:
                      switch:
                        - condition: ${flag}
                          return: "from-branch"
`))
		if err == nil {
			t.Fatal("should fail to compile a return step nested in a parallel branch")
		}
		if !strings.Contains(err.Error(), "return is not allowed in a parallel branch") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("in a parallel iteration", func(t *testing.T) {
		_, err := workflow.ParseWorkflowYAML(strings.NewReader(`
main:
  params: [args]
  steps:
    - par:
        parallel:
          for:
            value: v
            in: [1, 2, 3]
            steps:
              - r1:
                  return: ${v}
`))
		if err == nil {
			t.Fatal("should fail to compile a return step in a parallel iteration")
		}
		if !strings.Contains(err.Error(), "return is not allowed in a parallel iteration") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestSerialForReturn(t *testing.T) {
	ret, err := executeWorkflowYAML(t, `
main:
  params: [args]
  steps:
    - loop:
        for:
          value: v
          in: [1, 2, 3]
          steps:
            - check:
                switch:
                  - condition: ${v == 2}
                    return: ${v * 10}
    - done:
        return: "not found"
`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ret != int64(20) {
		t.Errorf("should return 20 but got %v (%T)", ret, ret)
	}
}
