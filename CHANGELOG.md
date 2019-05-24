# Changelog

## 0.3.2 (2019-06-11)

- Transmit body directly if request is not chunked

## 0.3.1 (2019-05-29)

- Move to uvicorn, for clearer documentation about chunked requests

## 0.2.1 (2019-05-22)

- Migrate to Falcon instead of Starlette, for easier handling of any incoming request
- Better request-level logging into files
- IP-level authorization (allow or not users based on incoming IP)
  This is required because clickhouse user authorization is based on username, password, and incoming IP. Since the proxy will connect from its own authorized IP, the actual IP will be hidden, so the proxy should handle the IP authorization logic.

## 0.1.1 (2019-05-20)

- Only change request body if there is a request body
- Start changelog
