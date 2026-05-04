package handlers

import "net/http"

type InvokeLifecycle struct {
	autoScalerController *FaasdAutoScalerController
	callGraphController  *FaasdCallGraphController
}

func NewInvokeLifecycle(autoScalerController *FaasdAutoScalerController, callGraphController *FaasdCallGraphController) *InvokeLifecycle {
	return &InvokeLifecycle{autoScalerController, callGraphController}
}

func (i *InvokeLifecycle) StartInvocation(r *http.Request, functionName string) error {
	fnName, namespace := ParseFunctionNameNamespace(functionName)

	if i != nil && i.callGraphController != nil && i.callGraphController.Enabled() {
		i.callGraphController.StartInvocation(r, namespace, fnName)
	}

	if i != nil && i.autoScalerController != nil && i.autoScalerController.Enabled() {
		return i.autoScalerController.StartInvocation(namespace, fnName)
	}
	return nil
}

func (i *InvokeLifecycle) EndInvocation(r *http.Request, functionName string) {
	fnName, namespace := ParseFunctionNameNamespace(functionName)

	if i != nil && i.callGraphController != nil && i.callGraphController.Enabled() {
		i.callGraphController.EndInvocation(r, namespace, fnName)
	}

	if i != nil && i.autoScalerController != nil && i.autoScalerController.Enabled() {
		i.autoScalerController.EndInvocation(namespace, fnName)
	}
}
