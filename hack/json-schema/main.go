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
	"encoding/json"
	"fmt"
	"os"
)

const (
	group        = "numaflow.numaproj.io"
	version      = "v1alpha1"
	pipelineKind = "Pipeline"
	vertexKind   = "Vertex"
	isbSvcKind   = "InterStepBufferService"
)

type obj = map[string]interface{}

func main() {
	swagger := obj{}
	{
		f, err := os.Open("api/openapi-spec/swagger.json")
		if err != nil {
			panic(err)
		}
		err = json.NewDecoder(f).Decode(&swagger)
		if err != nil {
			panic(err)
		}
	}
	{
		crdKinds := []string{
			isbSvcKind,
			pipelineKind,
			vertexKind,
		}
		definitions := swagger["definitions"]
		oneOf := make([]obj, 0, len(crdKinds))
		for _, kind := range crdKinds {
			definitionKey := fmt.Sprintf("io.numaproj.numaflow.%s.%s", version, kind)
			v := definitions.(obj)[definitionKey].(obj)
			v["x-kubernetes-group-version-kind"] = []obj{
				{
					"group":   group,
					"kind":    kind,
					"version": version,
				},
			}
			props := v["properties"].(obj)
			props["apiVersion"].(obj)["const"] = fmt.Sprintf("%s/%s", group, version)
			props["kind"].(obj)["const"] = kind
			oneOf = append(oneOf, obj{"$ref": "#/definitions/" + definitionKey})
		}

		schema := obj{
			"$id":         "http://io.numaproj.numaflow/numaflow.json",
			"$schema":     "http://json-schema.org/schema#",
			"type":        "object",
			"oneOf":       oneOf,
			"definitions": definitions,
		}
		f, err := os.Create("api/json-schema/schema.json")
		if err != nil {
			panic(err)
		}

		e := json.NewEncoder(f)
		e.SetIndent("", "  ")
		err = e.Encode(schema)
		if err != nil {
			panic(err)
		}

		err = f.Close()
		if err != nil {
			panic(err)
		}
	}
}
