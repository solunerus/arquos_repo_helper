defmodule ArquosRepoHelper do
  defmacro __using__(opts) do
    current_repo = Keyword.get(opts, :connection_data, nil)

    quote do
      import ArquosRepoHelper.Server
    end
  end
end
