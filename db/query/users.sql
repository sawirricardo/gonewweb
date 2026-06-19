-- name: CreateUser :one
INSERT INTO users (
  name,
  email,
  password
) VALUES (
  sqlc.arg(name),
  sqlc.arg(email),
  sqlc.arg(password)
)
RETURNING id, name, email, password, created_at, updated_at, deleted_at;

-- name: GetUser :one
SELECT id, name, email, password, created_at, updated_at, deleted_at
FROM users
WHERE id = sqlc.arg(id)
  AND deleted_at IS NULL;

-- name: GetUserByEmail :one
SELECT id, name, email, password, created_at, updated_at, deleted_at
FROM users
WHERE email = sqlc.arg(email)
  AND deleted_at IS NULL;

-- name: ListUsers :many
SELECT id, name, email, password, created_at, updated_at, deleted_at
FROM users
WHERE deleted_at IS NULL
ORDER BY id
LIMIT sqlc.arg(limit_count)
OFFSET sqlc.arg(offset_count);

-- name: UpdateUser :one
UPDATE users
SET
  name = sqlc.arg(name),
  email = sqlc.arg(email),
  password = sqlc.arg(password),
  updated_at = now()
WHERE id = sqlc.arg(id)
  AND deleted_at IS NULL
RETURNING id, name, email, password, created_at, updated_at, deleted_at;

-- name: DeleteUser :exec
UPDATE users
SET
  deleted_at = now(),
  updated_at = now()
WHERE id = sqlc.arg(id)
  AND deleted_at IS NULL;
