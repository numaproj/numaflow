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
			statusObj := properties[k].(obj)
			properties[k] = updateStatus(statusObj, filename)
		}
		if k == "spec" {
			specObj := properties[k].(obj)
			properties[k] = updateSpec(specObj, filename)
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

// Update the "status" fields
// We can remove all subfields which are not referenced elsewhere in the CRD
func updateStatus(statusObj obj, filename string) obj {
	var statusPropsObj obj

	// For the "monovertices" and "vertices" CRDs, there are fields which are referenced in the subresources.scale section, so those should be defined still.
	if strings.HasSuffix(filename, "_monovertices.yaml") || strings.HasSuffix(filename, "_vertices.yaml") {

		// Keep all status fields, but replace non-preserved ones with x-kubernetes-preserve-unknown-fields
		if statusProperties, exists := statusObj["properties"]; exists {
			statusPropsObj = statusProperties.(obj)

			// Replace each field except "selector" and "replicas" with preserve-unknown-fields
			// but only if the field is of type="object"
			for statusField := range statusPropsObj {
				if statusField != "selector" && statusField != "replicas" {
					if fieldObj, ok := statusPropsObj[statusField].(obj); ok {
						if fieldType, hasType := fieldObj["type"]; hasType && fieldType == "object" {
							statusPropsObj[statusField] = obj{"type": "object", "x-kubernetes-preserve-unknown-fields": true}
						}
					}
				}
			}
		}

		return statusObj
	} else {
		return obj{"type": "object", "x-kubernetes-preserve-unknown-fields": true}
	}

}

// Update the "spec" fields
// We can remove all subfields which are not referenced elsewhere in the CRD
func updateSpec(specObj obj, filename string) obj {
	var specPropsObj obj

	if strings.HasSuffix(filename, "_monovertices.yaml") || strings.HasSuffix(filename, "_vertices.yaml") {

		// Keep all spec fields, but replace non-preserved ones with x-kubernetes-preserve-unknown-fields
		if specProperties, exists := specObj["properties"]; exists {
			specPropsObj = specProperties.(obj)

			// Replace each field except "replicas" with preserve-unknown-fields
			// but only if the field is of type="object"
			for specField := range specPropsObj {
				if specField != "replicas" {
					if fieldObj, ok := specPropsObj[specField].(obj); ok {
						if fieldType, hasType := fieldObj["type"]; hasType && fieldType == "object" {
							specPropsObj[specField] = obj{"type": "object", "x-kubernetes-preserve-unknown-fields": true}
						}
					}
				}
			}
		}

		return specObj
	} else {
		return obj{"type": "object", "x-kubernetes-preserve-unknown-fields": true}
	}

}
