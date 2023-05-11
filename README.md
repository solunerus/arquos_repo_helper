# ArquosRepoHelper

## Motivo

El proyecto sirve como alternativa para ejecutar scripts (ej. procedimientos almacenados) de  MS-SQL Server.

## Instalación

La integración de este paquete es sencilla, basta con copiar la siguiente línea en su función `deps` en su archivo `mix.exs`:

```elixir
def deps do
  [
    {:arquos_repo_helper, git: "https://github.com/solunerus/arquos_repo_helper.git", tag: "v0.1.2"}
  ]
end
```

### Uso en Phoenix Framework

Para hacer uso de este paquete en Phoenix Framework se tiene que agregar la siguiente línea en el archivo `repo.ex`:

```elixir
defmodule MyProjectName.Repo do
  use Ecto.Repo,
    otp_app: :my_project_name,
    adapter: Ecto.Adapters.Tds

    use ArquosRepoHelper, connection_data: Application.get_env(:my_project_name, MyProjectName.Repo) # Línea que se debe añadir
end
```
