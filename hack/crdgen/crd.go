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

func updateStatus(statusObj obj, filename string) obj {
	var statusPropsObj obj

	if strings.HasSuffix(filename, "_monovertices.yaml") || strings.HasSuffix(filename, "_vertices.yaml") {
		// For status, keep only the "selector" and "replicas" fields and set additionalProperties: true
		newStatusObj := obj{
			"type":                 "object",
			"additionalProperties": true,
		}

		if statusProperties, exists := statusObj["properties"]; exists {
			statusPropsObj = statusProperties.(obj)

			// Create new properties with only selector and replicas if they exist
			newProps := make(obj)
			if selector, hasSelector := statusPropsObj["selector"]; hasSelector {
				newProps["selector"] = selector
			}
			if replicas, hasReplicas := statusPropsObj["replicas"]; hasReplicas {
				newProps["replicas"] = replicas
			}

			// Only add properties if we have any to add
			if len(newProps) > 0 {
				newStatusObj["properties"] = newProps
			}
		}

		return newStatusObj
	} else {
		statusPropsObj = obj{"type": "object", "x-kubernetes-preserve-unknown-fields": true}
	}

	return statusPropsObj
}

func updateSpec(specObj obj, filename string) obj {
	var specPropsObj obj

	if strings.HasSuffix(filename, "_monovertices.yaml") || strings.HasSuffix(filename, "_vertices.yaml") {
		// For spec, keep only the "replicas" field and set additionalProperties: true
		newSpecObj := obj{
			"type":                 "object",
			"additionalProperties": true,
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
