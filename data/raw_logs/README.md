Файлы должны содержать логи в Apache Common Log Format:

## Структура:
- `IP` - IP адрес клиента
- `user` - идентификатор пользователя (`-` если анонимный)
- `[timestamp]` - время запроса
- `"method endpoint protocol"` - HTTP запрос
- `status` - HTTP статус ответа
- `bytes` - размер ответа в байтах

## Пример файла:
`sample_logs.csv` с тестовыми данными.