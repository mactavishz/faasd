package handlers

import "net/http"

type InvokeLifecycle struct {
	controller *FaasdAutoScaler
}

func NewInvokeLifecycle(controller *FaasdAutoScaler) *InvokeLifecycle {
	return &InvokeLifecycle{controller: controller}
}

func (i *InvokeLifecycle) StartInvocation(_ *http.Request, functionName string) error {
	if i == nil || i.controller == nil || !i.controller.Enabled() {
		return nil
	}

	fnName, namespace := ParseFunctionNameNamespace(functionName)
	return i.controller.StartInvocation(namespace, fnName)
}

func (i *InvokeLifecycle) EndInvocation(_ *http.Request, functionName string) {
	if i == nil || i.controller == nil || !i.controller.Enabled() {
		return
	}

	fnName, namespace := ParseFunctionNameNamespace(functionName)
	i.controller.EndInvocation(namespace, fnName)
}
