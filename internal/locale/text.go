package locale

import (
	_ "embed"
	"encoding/json"
)

//go:embed zh.json
var chineseJSON []byte
var chinese = func() map[string]string {
	var values map[string]string
	if err := json.Unmarshal(chineseJSON, &values); err != nil {
		panic(err)
	}
	return values
}()

// Text localizes fixed presentation copy only. Dynamic content stays untouched.
func Text(language, english string) string {
	if language == "zh" {
		if value := chinese[english]; value != "" {
			return value
		}
	}
	return english
}
func Translator(language string) func(string) string {
	return func(english string) string { return Text(language, english) }
}
