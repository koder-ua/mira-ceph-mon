package main

import "encoding/json"

func parseJSON(data []byte) (*map[string]interface{}, error) {
	var cephS map[string]interface{}
	return &cephS, json.Unmarshal(data, &cephS)
}

func getJSONField(root interface{}, path ...string) interface{} {
	var curr interface{}
	curr = root

	for _, v := range path {
		switch vl := curr.(type) {
		case *map[string]interface{}:
			if val, ok := (*vl)[v]; ok {
				curr = val
			} else {
				return nil
			}
		case map[string]interface{}:
			if val, ok := vl[v]; ok {
				curr = val
			} else {
				return nil
			}
		default:
			panic("Unknown type")
		}

	}

	return curr
}

func getJSONFieldStr(root interface{}, path ...string) string {
	return getJSONField(root, path...).(string)
}

func getJSONFieldFloat(root interface{}, path ...string) float64 {
	return getJSONField(root, path...).(float64)
}

func getJSONFieldInt(root interface{}, path ...string) int {
	return getJSONField(root, path...).(int)
}

