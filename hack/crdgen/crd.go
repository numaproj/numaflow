/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"os"
	"strings"

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
		if k == "status" {
			properties[k] = obj{"type": "object", "x-kubernetes-preserve-unknown-fields": true}
		}
		if k == "spec" {
			specObj := properties[k].(obj)
			properties[k] = updateSpecProperties(specObj, filename)
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

// Update the "spec.properties" object
// We can remove all subfields which are not referenced elsewhere in the CRD
func updateSpecProperties(specObj obj, filename string) obj {
	var specPropsObj obj

	// For the "monovertices" and "vertices" CRDs, the spec.replicas field referenced in the subresources.scale section
	// needs to be defined and defaulted to 1, to prevent nil error in HPA
	if strings.HasSuffix(filename, "_monovertices.yaml") || strings.HasSuffix(filename, "_vertices.yaml") {

		// keep only the "replicas" field defined
		newSpecObj := obj{
			"type":                                 "object",
			"x-kubernetes-preserve-unknown-fields": true,
		}

		if specProperties, exists := specObj["properties"]; exists {
			specPropsObj = specProperties.(obj)

			// Create new properties with only replicas if it exists
			newProps := make(obj)
			if replicas, hasReplicas := specPropsObj["replicas"]; hasReplicas {
				newProps["replicas"] = replicas
			}

			// Only add properties if we have any to add
			if len(newProps) > 0 {
				newSpecObj["properties"] = newProps
			}
		}

		return newSpecObj
	} else {
		specPropsObj = obj{"type": "object", "x-kubernetes-preserve-unknown-fields": true}
	}

	return specPropsObj
}
