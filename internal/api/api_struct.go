package api

type Request struct {
	Database string           `json:"database"`        // имя бд
	Command  string           `json:"operation"`       // операция
	Data     []map[string]any `json:"data,omitempty"`  // данные
	Query    map[string]any   `json:"query,omitempty"` // условия поиска
}

type Response struct {
	Status  string           `json:"status"`            // success или error
	Message string           `json:"message,omitempty"` // сообщение, если есть ошибка
	Data    []map[string]any `json:"data,omitempty"`    // результат запроса
	Count   int              `json:"count,omitempty"`   // количество документов
}

const (
	StatusSuccess = "success"
	StatusError   = "error"
)

const (
	CmdInsert      = "insert"
	CmdFind        = "find"
	CmdDelete      = "delete"
	CmdCreateIndex = "create_index"
)
