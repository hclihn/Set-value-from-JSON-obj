package main

import (
	"encoding/json"
	"fmt"
  "reflect"
  "strings"
  "strconv"
  "regexp"
)

type PathInfo struct {
	Delimiter string   // path delimiter
	Paths     []string // path components
}

func (p *PathInfo) splitPath(path string) {
	path = strings.TrimSpace(path)
	if path == "" {
		return
	}
	if p.Delimiter == "" {
		if strings.Contains(path, ".") {
			p.Delimiter = "."
		} else {
			p.Delimiter = "/"
		}
	}
	path = strings.Trim(path, p.Delimiter)
	if path == "" {
		return
	}
	if p.Paths == nil {
		p.Paths = make([]string, 0)
	}
	// trim leading and trailing spaces out of each path element, empty ones ignored
	for _, pp := range strings.Split(path, p.Delimiter) {
		s := strings.TrimSpace(pp)
		if s == "" {
			continue
		}
		p.Paths = append(p.Paths, s)
	}
	return
}

func (p PathInfo) FormatPath(idx int) string {
	if idx > len(p.Paths) { // truncate it
		idx = len(p.Paths)
	} else if idx < 0 { // use all path elements
		idx = len(p.Paths)
	}
	return p.Delimiter + strings.Join(p.Paths[:idx], p.Delimiter)
}

func NewPathInfo(path string) *PathInfo {
	pi := PathInfo{}
	pi.splitPath(path)
	return &pi
}

var zeroVal = reflect.Value{}

func GetVRefObj(t reflect.Value) reflect.Value {
getValue:
	for { // move through interface and pointers to get to the actual object
		switch t.Kind() {
		case reflect.Interface, reflect.Ptr:
			t = t.Elem()
		default:
			break getValue
		}
	}
	return t
}

func UnquoteString(s string, quote byte) (string, error) {
	var buf strings.Builder
	n, l := 0, len(s)
	for len(s) > 0 {
		v, _, ta, err := strconv.UnquoteChar(s, '"')
		if err != nil {
			return "", fmt.Errorf("failed to unquote string %q at index %d: %w", s, n, err)
		}
		buf.WriteRune(v)
		n += l - len(ta)
		s = ta
	}
	return buf.String(), nil
}

// ParseTag regexp patterns
var (
	tagConcatPtn = regexp.MustCompile(`"\s*\+\s*"`)                           // contatenate quoted strings
	tagParsePtn  = regexp.MustCompile(`\s*([^:]+)\s*:\s*"((?:[^"\\]|\\.)*)"`) // parse key-value pairs
)

func ParseTag(tag reflect.StructTag) (map[string]string, error) {
	tg := string(tag)
	tg = tagConcatPtn.ReplaceAllString(tg, "")       // contatenate quoted strings
	mat := tagParsePtn.FindAllStringSubmatch(tg, -1) // parse key-value pairs
	tm := make(map[string]string)
	for _, m := range mat {
		s, err := UnquoteString(m[2], '"')
		if err != nil {
			return nil, fmt.Errorf("failed to unquote key %q value %q when parsing struct tag %q: %w",
				m[1], m[2], tag, err)
		}
		tm[m[1]] = s
	}
	return tm, nil
}

func FindElement(v reflect.Value, idx int, pi *PathInfo) (reflect.Value, error) {
  if idx == len(pi.Paths) {
    //fmt.Printf("--> %q rtn: Kind=%s %#v\n", pi.FormatPath(idx), v.Kind(), v)
    return v, nil
  } else if idx > len(pi.Paths) {
    return zeroVal, fmt.Errorf("path index %d exceeding length of path (%d)", idx, len(pi.Paths))
  }
  v = GetVRefObj(v)
  p := pi.Paths[idx]

  //fmt.Printf("-> %q of %q: Kind=%s\n", p, pi.FormatPath(idx), v.Kind())
  switch v.Kind() {
	case reflect.Map: // for JSON Obj
    vp := reflect.ValueOf(p)
    kt := v.Type().Key() // key type
    if !vp.Type().ConvertibleTo(kt) {
      return zeroVal, fmt.Errorf("unable to convert path element %q type (%s) to map key type (%s) at path %q", p, vp.Type(), kt, pi.FormatPath(idx))
    }
    vp = vp.Convert(kt)
    val := v.MapIndex(vp)
    if val == zeroVal {
      return val, fmt.Errorf("path element %q does not exist in map at path %q", p, pi.FormatPath(idx))
    }
    return FindElement(val, idx+1, pi)
  case reflect.Struct: // for data struct
    fields := reflect.VisibleFields(v.Type())
    for _, field := range fields {
      n := field.Name
      // convert to JSON name if applicable
      if m, err := ParseTag(field.Tag); err != nil {
        return zeroVal, fmt.Errorf("failed to parse struct field %q tag (%q): %w", n, field.Tag, err)
      } else if tName, ok := m["json"]; ok {
        n = tName
      }
      if n != p {
        continue
      }
      if !field.IsExported() {
        return zeroVal, fmt.Errorf("path element %q is not an exported struct field (%s) at path %q", p, field.Name, pi.FormatPath(idx))
      }
      val := v.FieldByIndex(field.Index)
      if val.Kind() == reflect.Ptr && val.IsNil() { // nil pointer
        //fmt.Printf("--> %q: nil pointer, %#v\n", p, field)
        newVal := reflect.New(field.Type.Elem())
        val.Set(newVal)
        //fmt.Printf("--> %q: kind=%s, %#v\n", p, newVal.Elem().Type(), newVal)
        val = newVal
      }
      return FindElement(val, idx+1, pi)
    }
    return zeroVal, fmt.Errorf("path element %q does not exist in struct at path %q", p, pi.FormatPath(idx))
  default:
    return zeroVal, fmt.Errorf("unable to traverse into the path element %q at path %q: invalid data type (%s)", p, pi.FormatPath(idx), v.Kind())
  }
}

func ExtractFromJSONObj(jObj interface{}, names []string, target interface{}) error {
  rv := reflect.ValueOf(target)
  if rv.Kind() != reflect.Ptr || rv.IsNil() {
    return fmt.Errorf("invalid target (%T) specified: expected to be a non-nil pointer", target)
  }
  if names == nil { // full extraction
    if b, err := json.Marshal(jObj); err != nil {
  		return fmt.Errorf("failed to serialize jObj to JSON: %w", err)
  	} else if err := json.Unmarshal(b, target); err != nil {
  		return fmt.Errorf("failed to set value of jObj to target: %w", err)
  	}
    return nil
  }
  rv = reflect.Indirect(rv)
  rj := reflect.ValueOf(jObj)
  for _, name := range names {
    //fmt.Printf("-> Set value for path %q\n", name)
    pi := NewPathInfo(name)
    jv, err := FindElement(rj, 0, pi)
    if err != nil {
      return fmt.Errorf("failed to find JSON object from path %q: %w", name, err)
    }
    tv, err := FindElement(rv, 0, pi)
    if err != nil {
      return fmt.Errorf("failed to find struct object from path %q: %w", name, err)
    }
    if b, err := json.Marshal(jv.Interface()); err != nil {
  		return fmt.Errorf("failed to serialize JOSN object at path %q: %w", name, err)
  	} else {
      if tv.Kind() != reflect.Ptr {
        tv = tv.Addr()
      }
      if err := json.Unmarshal(b, tv.Interface()); err != nil {
  		  return fmt.Errorf("failed to set value of target struct at path %q: %w", name, err)
      }
  	}
  }
  return nil
}

func ExtractValueFromJSONObj(jObj interface{}, name string, target interface{}) error {
  rv := reflect.ValueOf(target)
  if rv.Kind() != reflect.Ptr || rv.IsNil() {
    return fmt.Errorf("invalid target (%T) specified: expected to be a non-nil pointer", target)
  }
  var v interface{}
  if t, ok := jObj.(map[string]interface{}); !ok {
		return fmt.Errorf("jObj (%T) is not map[string]interface{}", jObj)
	} else if name == "" {
    v = jObj
  } else if v, ok = t[name]; !ok {
		return fmt.Errorf("name %q does not exist in jObj", name)
	}
  
  if b, err := json.Marshal(v); err != nil {
		return fmt.Errorf("failed to serialize jObj[%s] to JSON: %w", name, err)
	} else if err := json.Unmarshal(b, target); err != nil {
		return fmt.Errorf("failed to set value of jObj[%s] to target: %w", name, err)
	}
	return nil
}

func ExtractValuesFromJSONObj(jObj interface{}, names []string, target interface{}) error {
  rv := reflect.ValueOf(target)
  if rv.Kind() != reflect.Ptr || rv.IsNil() {
    return fmt.Errorf("invalid target (%T) specified: expected to be a non-nil pointer", target)
  }
  rv = reflect.Indirect(rv)
  fields := reflect.VisibleFields(rv.Type())
  nameMap := make(map[string]struct{})
  for _, name := range names {
    nameMap[name] = struct{}{}
  }
  for _, field := range fields {
    n := field.Name
    if _, ok := nameMap[n]; !ok {
      continue
    }
    if !field.IsExported() {
      return fmt.Errorf("field %q is not exported", n)
    }
    if tName := field.Tag.Get("json"); tName != "" {
      n = tName
    }
    if err := ExtractValueFromJSONObj(jObj, n, rv.FieldByIndex(field.Index).Addr().Interface()); err != nil {
      return fmt.Errorf("failed to extract field %q: %w", n, err)
    }
  }
  return nil
}

type FormatProber interface {
	ParseFormatVersion() error // Parses the FormatVersion field
	GetVersion() int           // Returns manifest's version number
	Format() string            // Returns manifest's format string
}

type Manifest struct {
	FormatProber
}

func (m Manifest) MarshalJSON() ([]byte, error) {
	// eliminate the FormatProber level, just the manifest data
	return json.Marshal(m.FormatProber)
}

func (m *Manifest) UnmarshalJSON(b []byte) (err error) {
	if string(b) == "null" { // nil FormatProber
		return
	}
	(*m).FormatProber, err = GetManifestData("Manifest", b)
	return
}

type FormatProbe struct {
	FormatVersion string `cabdoc:"The string representing format:version" cabflag:"ro"`
	// the following are not exported
	format  string
	version int
}

var _ FormatProber = (*FormatProbe)(nil)

func (p *FormatProbe) ParseFormatVersion() error {
	p.FormatVersion = strings.TrimSpace(p.FormatVersion)
	if p.FormatVersion == "" {
		return fmt.Errorf("failed to parse formatVersion: empty field")
	}
	fields := strings.Split(p.FormatVersion, ":")
	if len(fields) != 2 {
		return fmt.Errorf(
			"failed to parse formatVersion %q: invalid format, expected <format>:<version>", p.FormatVersion)
	}
	p.format = fields[0]
	v, err := strconv.Atoi(fields[1])
	if err != nil {
		return fmt.Errorf("failed to get version in formatVersion %q: %w", p.FormatVersion, err)
	}
	p.version = v
	return nil
}

func (p FormatProbe) GetVersion() int {
	return p.version
}

func (p FormatProbe) Format() string {
	return p.format
}

func ProbeManifest(name string, manifest []byte) (*FormatProbe, error) {
	var p FormatProbe
	if err := json.Unmarshal(manifest, &p); err != nil {
		return nil, fmt.Errorf("failed to probe manifest %q: %w", name, err)
	}
	if err := p.ParseFormatVersion(); err != nil {
		return nil, fmt.Errorf("failed to probe manifest %q: %w", name, err)
	}
	return &p, nil
}

func GetManifestData(name string, manifest []byte) (FormatProber, error) {
	var err error
	p, err := ProbeManifest(name, manifest)
	if err != nil {
		return nil, err
	}
	// get the correct data object
  var m ImageManifestV1
	mObj := FormatProber(&m)
	// populate with JSON data
	if err = json.Unmarshal(manifest, mObj); err != nil {
		return nil, fmt.Errorf("failed to populate JSON data to %q manifest object: %w", p.Format(), err)
	}
	// get format and version info
	if err = mObj.ParseFormatVersion(); err != nil {
		return nil, fmt.Errorf("failed to get manifest format and version for %q: %w", name, err)
	}
	return mObj, nil
}

type ImageManifestV1 struct {
	// embed the anonymous struct here to take the FormatVersion field and associated information
	FormatProbe `cabdoc:"This is a long test string "+
  "with a long line!"`
	Env EnvInfo
}

type EnvInfo struct {
	Name      string `cabdoc:"how about this with a super "+
  "long line" json:"name"`
	URLPrefix string `cabdoc:"test this one w/ a NL" 
  json:"url_prefix"`
}

type CabServerConfig struct {
	URLPrefix             string    `json:"url_prefix"`
	SupportsGzippedUpload bool      `json:"supports_gzipped_upload"`
  ChunkSize uint `json:"chunk_size"`
	SupportsLogAppend     bool      `json:"supports_log_append"`
	NotifyServer          bool      `json:"notify_server"`
	Env                   []EnvInfo `json:"env"`
  Prod *EnvInfo `json:"prod"`
}

type ContextInfo struct {
  Status             string
  CabSvrConfig *CabServerConfig
  ImageManifest     Manifest
  UpCount            int   
  NotHere bool
}

func main() {
	var jObj interface{}
	if err := json.Unmarshal([]byte(config), &jObj); err != nil {
    fmt.Println("ERROR:", err)
    return
  }
	fmt.Printf("Config jObj: %#v\n", jObj)
	
  { var cfg CabServerConfig
    fmt.Println("\nPartial extractions...")
  	if err := ExtractValueFromJSONObj(jObj, "env", &cfg.Env); err != nil {
      fmt.Println("ERROR:", err)
      return
    }
  	if err := ExtractValueFromJSONObj(jObj, "url_prefix", &cfg.URLPrefix); err != nil {
      fmt.Println("ERROR:", err)
      return
    }
  	fmt.Printf("config (partial extraction): %+v\n", cfg)
  }
  { var cfg CabServerConfig
    fmt.Println("\nFull extraction...")
    if err := ExtractValueFromJSONObj(jObj, "", &cfg); err != nil {
      fmt.Println("ERROR:", err)
      return
    }
    fmt.Printf("config (full extraction): %+v\n", cfg)
  }
  { var cfg CabServerConfig
    fmt.Println("\nExtraction by names...")
    if err := ExtractValuesFromJSONObj(jObj, []string{"URLPrefix","Env"}, &cfg); err != nil {
      fmt.Println("ERROR:", err)
      return
    }
    fmt.Printf("config (extraction by names): %+v\n", cfg)
  }
  { var cfg CabServerConfig
    fmt.Println("\nExtraction by paths...")
    if err := ExtractFromJSONObj(jObj, []string{"url_prefix","prod/url_prefix", "env", "chunk_size", "supports_gzipped_upload"}, &cfg); err != nil {
      fmt.Println("ERROR:", err)
      return
    }
    fmt.Printf("config (extraction by paths): %#v, %#v\n", cfg, cfg.Prod)
  }
  { var cfg CabServerConfig
    fmt.Println("\nFull extraction by path...")
    if err := ExtractFromJSONObj(jObj, nil, &cfg); err != nil {
      fmt.Println("ERROR:", err)
      return
    }
    fmt.Printf("config (full extraction by path): %#v, %#v\n", cfg, cfg.Prod)
  }


  if err := json.Unmarshal([]byte(context), &jObj); err != nil {
    fmt.Println("ERROR:", err)
    return
  }
	fmt.Printf("Context jObj: %#v\n", jObj)
  { var cfg ContextInfo
    fmt.Println("\nContext Extraction by paths...")
    if err := ExtractFromJSONObj(jObj, []string{"CabSvrConfig","ImageManifest"}, &cfg); err != nil {
      fmt.Println("ERROR:", err)
      return
    }
    fmt.Printf("context (extraction by paths): %#v, \nSvrCfg:%#v, \nSvrCfg.Prod:%#v, \nImageManifest:%#v\n", cfg, cfg.CabSvrConfig, cfg.CabSvrConfig.Prod, cfg.ImageManifest.FormatProber.(*ImageManifestV1))
  }
  { var cfg ContextInfo
    fmt.Println("\nContext Full extraction by path...")
    if err := ExtractFromJSONObj(jObj, nil, &cfg); err != nil {
      fmt.Println("ERROR:", err)
      return
    }
    fmt.Printf("context (full extraction by path): %#v, \nSvrCfg:%#v, \nSvrCfg.Prod:%#v, \nImageManifest:%#v\n", cfg, cfg.CabSvrConfig, cfg.CabSvrConfig.Prod, cfg.ImageManifest.FormatProber.(*ImageManifestV1))
  }
}

const (
  config = `{
  "url_prefix": "A5.1-rc3",
  "supports_gzipped_upload": true,
  "supports_log_append": false,
  "notify_server": false,
  "chunk_size": 10240,
  "env": [
    {
      "name": "hcl_test",
      "url_prefix": "hcl_test"
    } 
  ],
  "prod": {
    "name": "hcl_test1",
    "url_prefix": "hcl_test1"
  }
}
`
  context = `{
  "Status": "Test this one!",
  "CabSvrConfig": {
    "url_prefix": "A5.1-rc3",
    "supports_gzipped_upload": true,
    "supports_log_append": false,
    "notify_server": false,
    "chunk_size": 10240,
    "env": [
      {
        "name": "hcl_test",
        "url_prefix": "hcl_test"
      } 
    ],
    "prod": {
      "name": "hcl_test2",
      "url_prefix": "hcl_test2"
    }
  },
  "UpCount": 12,
  "ImageManifest": {
    "FormatVersion": "image:1",
    "Env": {
      "name": "hcl_test14",
      "url_prefix": "hcl_test14"
    }
  },
  "Extra": true
}
`
)
