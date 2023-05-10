defmodule ArquosRepoHelper.Server do
  import ArquosRepoHelper.Core
  def script(qry, :many), do: sql_script(unquote(current_repo), qry, :many)
  def script(qry), do: sql_script(unquote(current_repo), qry)

  def execute(qry, :many), do: sql_execute(unquote(current_repo), qry, :many)
  def execute(qry), do: sql_execute(unquote(current_repo), qry)
  def execute(qry, toStruct), do: sql_execute(unquote(current_repo), qry, toStruct)

  def map_to_xml(map, table_name), do: struct_to_xml(map, table_name)
  def revisa_bitacora(bitacora), do: check_log(bitacora)
end
