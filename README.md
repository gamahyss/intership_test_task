# Payment proof service

Небольшой Python-сервис для задания «Платёж прошёл? Докажи». Сервис хранит операции в SQLite, сначала фиксирует намерение отправки, затем вызывает provider-simulator с постоянным `Idempotency-Key`, принимает callback-квитанции и восстанавливает незавершённые отправки после перезапуска.

## Запуск

```bash
docker compose up --build
```

Сервис кандидата слушает `http://localhost:8080`, симулятор провайдера — `http://localhost:8081`. Данные сервиса сохраняются в volume `candidate-data`.

## Сквозной сценарий

```bash
curl -i http://localhost:8080/health

curl -i -X POST http://localhost:8080/operations \
  -H 'Content-Type: application/json' \
  -d '{
    "operationId": "operation-123",
    "amount": "1000.00",
    "currency": "RUB",
    "description": "Оплата заказа"
  }'

curl -i -X POST http://localhost:8080/operations/operation-123/submit

curl -i http://localhost:8080/operations/operation-123
curl -i http://localhost:8080/operations/operation-123/events
```

Повторный `submit` безопасен:

```bash
curl -i -X POST http://localhost:8080/operations/operation-123/submit
```

Проверка идемпотентности callback:

```bash
curl -i -X POST http://localhost:8080/operations \
  -H 'Content-Type: application/json' \
  -d '{
    "operationId": "operation-manual",
    "amount": "100.00",
    "currency": "RUB",
    "description": "Ручная проверка квитанции"
  }'

curl -i -X POST http://localhost:8080/operations/operation-manual/submit

curl -i -X POST http://localhost:8080/receipts \
  -H 'Content-Type: application/json' \
  -d '{
    "providerPaymentId": "manual-provider-payment-id",
    "operationId": "operation-manual",
    "result": "COMPLETED",
    "message": "Payment completed",
    "occurredAt": "2026-07-15T12:00:00Z"
  }'
```

## Реализованный контракт

- `GET /health`
- `POST /operations`
- `POST /operations/{id}/submit`
- `POST /receipts`
- `GET /operations/{id}`
- `GET /operations/{id}/events`

Важные свойства:

- `submit` в одной транзакции переводит `CREATED -> PROCESSING` и сохраняет тело будущего запроса провайдеру.
- Внешний HTTP-вызов выполняется после коммита намерения, без удержания блокировки операции.
- Все повторы используют `Idempotency-Key: operationId` и `X-Correlation-ID: operationId`.
- Ответ `202` от провайдера сохраняет `providerPaymentId`, но не переводит операцию в финальный статус.
- Финальные статусы выставляются только по callback-квитанциям `COMPLETED` или `REJECTED`.
- Повторная квитанция не создаёт второй переход, поздняя противоположная квитанция фиксируется как ignored-событие.
- При рестарте незавершённые `PROCESSING`-операции снова отправляются с тем же ключом идемпотентности.
