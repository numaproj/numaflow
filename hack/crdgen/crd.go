package main

import (
	"os"

	"sigs.k8s.io/yaml"
)

type obj = map[string]interface{}

func cleanCRD(filename string) {
	data, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	crd := make(map[string]interface{})
	err = yaml.Unmarshal(data, &crd)
	if err != nil {
		panic(err)
	}
	delete(crd, "status")
	metadata := crd["metadata"].(obj)
	delete(metadata, "annotations")
	delete(metadata, "creationTimestamp")
	spec := crd["spec"].(obj)
	delete(spec, "validation")
	versions := spec["versions"].([]interface{})
	version := versions[0].(obj)
	properties := version["schema"].(obj)["openAPIV3Schema"].(obj)["properties"].(obj)
	for k := range properties {
		if k == "spec" || k == "status" {
			properties[k] = obj{"type": "object", "x-kubernetes-preserve-unknown-fields": true}
		}
		if k == "apiVersion" || k == "kind" {
			o := properties[k].(obj)
			delete(o, "description")
		}
	}
	data, err = yaml.Marshal(crd)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(filename, data, 0666)
	if err != nil {
		panic(err)
	}
}
