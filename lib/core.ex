defmodule ArquosRepoHelper.Core do
  require Decimal

  defp tds_start(repo), do: Tds.start_link(repo)

  defp tds_query(connection, qry, :many),
    do: Tds.query_multi(connection, qry, _params = [], _values = [])

  defp tds_query(connection, qry, :one),
    do: Tds.query(connection, qry, _params = [], _values = [])

  defp stop_server(connection), do: GenServer.stop(connection, :normal)

  defp error_message(msg, :msg_conn), do: {:error, :connection_error, msg}

  defp error_message(msg, :sql_msg), do: {:error, :sql_error, msg}

  defp error_sql(msg_text, proc_name, server_name),
    do: {:error, :sql_error, "#{server_name}|#{proc_name}|#{msg_text}"}

  defp tds_tuple(columns, rows, num_rows), do: {:ok, tds_result(columns, rows, num_rows)}

  defp tds_result(columns, rows, num_rows),
    do: %Tds.Result{columns: columns, rows: rows, num_rows: num_rows}

  defp error_type_msg(type, msg), do: {:error, type, msg}

  defp case_sql_execute(data, resultset) do
    case data do
      {:ok, bitacora, _colum_names, _row_count} ->
        case check_log(bitacora) do
          {:ok, :empty, _msg} ->
            maybe_dataset =
              if(length(resultset) > 1, do: List.delete_at(resultset, -1), else: resultset)
              |> Stream.map(fn
                %Tds.Result{columns: lst_colum_names, rows: lst_data_rows, num_rows: row_count} ->
                  tds_result(lst_colum_names, lst_data_rows, row_count) |> tds_result_to_table()
              end)
              |> Enum.to_list()

            cond do
              length(maybe_dataset) == 0 ->
                {:error, :not_found, "Vacio"}

              length(maybe_dataset) == 1 ->
                hd(maybe_dataset)

              length(maybe_dataset) > 1 ->
                dataset =
                  maybe_dataset
                  |> Enum.map(fn
                    {:ok, data, _columns, _rowcount} -> data
                    {:error, type, msg} -> {:error, type, msg}
                  end)

                {:ok, dataset, :dataset}
            end

          {:ok, _data} ->
            {
              :ok,
              resultset
              |> Stream.map(fn
                %Tds.Result{columns: lst_colum_names, rows: lst_data_rows, num_rows: row_count} ->
                  tds_result(lst_colum_names, lst_data_rows, row_count) |> tds_result_to_table()
              end)
              |> Stream.map(fn
                {:ok, data, _columns, _rowcount} -> data
                {:error, type, msg} -> {:error, type, msg}
              end)
              |> Enum.to_list(),
              :dataset
            }

          elsedata ->
            elsedata
            |> IO.inspect(label: "elsedata")
        end

      _else ->
        data
        |> IO.inspect(label: "elsecase")
    end
  end

  defp tds_result_to_table(%{columns: columns, rows: rows, num_rows: num_rows} = _item) do
    cond do
      rows == nil ->
        {:ok, [], [], num_rows}

      num_rows == 0 ->
        {:error, :not_found, "NO se encontraron registros"}

      true ->
        record_set = rows |> remove_raw_binary() |> to_map(columns)
        {:ok, record_set, columns, num_rows}
    end
  end

  defp raw_binary_to_string(raw) do
    raw
    |> String.codepoints()
    |> Enum.reduce("", fn w, result ->
      cond do
        String.valid?(w) ->
          result <> w

        true ->
          <<parsed::8>> = w
          result <> <<parsed::utf8>>
      end
    end)
  end

  defp remove_raw_binary(lst_data_rows) do
    if length(lst_data_rows) > 0 do
      rows =
        lst_data_rows
        |> Stream.map(fn x ->
          Stream.map(x, fn i ->
            cond do
              is_nil(i) ->
                ""

              is_boolean(i) ->
                i

              is_number(i) or Decimal.is_decimal(i) ->
                input_str =
                  cond do
                    is_integer(i) ->
                      "#{i}"

                    is_float(i) ->
                      "#{i}"

                    Decimal.is_decimal(i) ->
                      i
                      |> Decimal.to_string()

                    true ->
                      "nil"
                  end

                # |> IO.inspect(label: "is_number")

                cond do
                  Regex.match?(~r/^[+-]?\d+$/, input_str) ->
                    String.to_integer(input_str)

                  Regex.match?(~r/^[+-]?\d+\.\d+([eE][+-]?\d+)?$/, input_str) ->
                    String.to_float(input_str)

                  true ->
                    ":error"
                end

              # |> IO.inspect(label: "is_number")
              is_maybe_date?(i) ->
                maybe_date_to_string(i)

              is_maybe_datetime?(i) ->
                maybe_datetime_to_string(i)

              is_naivedatetime?(i) ->
                NaiveDateTime.to_string(i)

              is_date?(i) ->
                Date.to_string(i)

              is_time?(i) ->
                Time.to_string(i)

              String.valid?(i) ->
                i

              true ->
                raw_binary_to_string(i)
            end
          end)
          |> Enum.to_list()
        end)

      Enum.to_list(rows)
    else
      lst_data_rows
    end
  end

  defp is_naivedatetime?(%NaiveDateTime{}), do: true
  defp is_naivedatetime?(_), do: false
  defp is_time?(%Time{}), do: true
  defp is_time?(_), do: false
  defp is_datetime?(%DateTime{}), do: true
  defp is_datetime?(_), do: false
  defp is_maybe_datetime?({{_yyyy, _mm, _dd}, {_hh, _mi, _ss, _mss}}), do: true
  defp is_maybe_datetime?(_), do: false
  defp is_maybe_date?({_yyyy, _mm, _dd}), do: true
  defp is_maybe_date?(_), do: false
  defp is_date?(%Date{}), do: true
  defp is_date?(_), do: false

  @spec float_to_binary(float) :: binary
  defp float_to_binary(f), do: float_to_binary(f, 4)

  @spec float_to_binary(float, integer) :: binary
  defp float_to_binary(f, nd) do
    fl = :erlang.float_to_binary(f, [:compact, {:decimals, nd}])
    [i, d] = fl |> String.split(".")
    d = d |> String.pad_trailing(nd, "0")
    i <> "." <> d
  end

  defp maybe_date_to_string({yyyy, mm, dd}) do
    year = Integer.to_string(yyyy)

    month = Integer.to_string(mm) |> String.pad_leading(2, "0")

    day = Integer.to_string(dd) |> String.pad_leading(2, "0")

    Enum.join([year, month, day], "-")
  end

  defp maybe_datetime_to_string({xdate, {hh, mi, ss, mss}}) do
    date = maybe_date_to_string(xdate)

    hra = Integer.to_string(hh) |> String.pad_leading(2, "0")

    min = Integer.to_string(mi) |> String.pad_leading(2, "0")

    sec = Integer.to_string(ss) |> String.pad_leading(2, "0")

    mse = Integer.to_string(mss) |> String.pad_leading(3, "0")

    date <> " " <> Enum.join([hra, min, sec], ":") <> "." <> mse
  end

  defp to_struct(lst_data_rows, lst_colum_names, toStruct) do
    data_rows =
      lst_data_rows
      |> Stream.map(fn record ->
        lst_colum_names
        |> Enum.map(fn s -> s |> String.to_atom() end)
        |> Stream.zip(record)
      end)
      |> Stream.map(fn value -> struct(toStruct, value) end)

    Enum.to_list(data_rows)
  end

  defp to_map(lst_data_rows, lst_colum_names) do
    data_rows =
      lst_data_rows
      |> Stream.map(fn record ->
        # records=
        lst_colum_names
        |> Stream.map(fn s -> s |> String.to_atom() end)
        |> Stream.zip(record)
        |> Enum.into(%{})
      end)

    Enum.to_list(data_rows)
  end

  defp typeof(self) do
    cond do
      is_float(self) -> "float"
      is_integer(self) -> "integer"
      Decimal.is_decimal(self) -> "decimal"
      is_number(self) -> "number"
      is_atom(self) -> "atom"
      is_boolean(self) -> "boolean"
      is_binary(self) -> "binary"
      is_function(self) -> "function"
      is_list(self) -> "list"
      is_tuple(self) -> "tuple"
      is_naivedatetime?(self) -> "naivedatetime"
      is_datetime?(self) -> "datetime"
      is_date?(self) -> "date"
      is_time?(self) -> "time"
      true -> self |> IO.inspect(label: "typeof unknow:")
    end
  end

  defp pow(n, k), do: pow(n, k, 1)
  defp pow(_, 0, acc), do: acc
  defp pow(n, k, acc), do: pow(n, k - 1, n * acc)

  defp is_upcase?(x), do: x == String.upcase(x)

  def sql_script(repo, qry, :many) do
    cond do
      String.length(qry) > 0 ->
        with {:ok, connection} <- tds_start(repo),
             {:ok, result_list} <- tds_query(connection, qry, :many) do
          stop_server(connection)
          {:ok, result_list, count: length(result_list)}
        else
          {:error, %DBConnection.ConnectionError{message: msg_text}} ->
            error_message(msg_text, :msg_conn)

          {:error,
           %Tds.Error{
             message: _message,
             mssql: %{
               class: _class,
               line_number: _line_number,
               msg_text: msg_text,
               number: _number,
               proc_name: proc_name,
               server_name: server_name,
               state: _state
             }
           }} ->
            error_sql(msg_text, proc_name, server_name)
        end

      true ->
        error_message("INSTRUCCION DE SQL VACIA", :sql_msg)
    end
  end

  def sql_script(repo, qry) do
    cond do
      String.length(qry) > 0 ->
        with {:ok, connection} <- tds_start(repo),
             result <- tds_query(connection, qry, :one),
             {:ok,
              %Tds.Result{columns: lst_colum_names, rows: lst_data_rows, num_rows: row_count}} <-
               result do
          stop_server(connection)

          tds_tuple(lst_colum_names, lst_data_rows, row_count)
        else
          {:error, %DBConnection.ConnectionError{message: msg_text}} ->
            error_message(msg_text, :msg_conn)

          {:error,
           %Tds.Error{
             message: _message,
             mssql: %{
               class: _class,
               line_number: _line_number,
               msg_text: msg_text,
               number: _number,
               proc_name: proc_name,
               server_name: server_name,
               state: _state
             }
           }} ->
            error_sql(msg_text, proc_name, server_name)
        end

      true ->
        error_message("INSTRUCCION DE SQL VACIA", :sql_msg)
    end
  end

  def sql_execute(repo, qry) do
    case sql_script(repo, qry) do
      {:error, type, msg} ->
        error_type_msg(type, msg)

      {:ok, %Tds.Result{columns: lst_colum_names, rows: lst_data_rows, num_rows: row_count}} ->
        tds_result(lst_colum_names, lst_data_rows, row_count) |> tds_result_to_table()
    end
  end

  def sql_execute(repo, qry, :many) do
    case sql_script(repo, qry, :many) do
      {:error, type, msg} ->
        error_type_msg(type, msg)

      {:ok, resultset, count: _count} ->
        with %Tds.Result{columns: lst_colum_names, rows: lst_data_rows, num_rows: row_count} <-
               List.last(resultset),
             data <-
               tds_result(lst_colum_names, lst_data_rows, row_count) |> tds_result_to_table() do
          case_sql_execute(data, resultset)
        end
    end
  end

  def sql_execute(repo, qry, toStruct) do
    case sql_script(repo, qry) do
      {:error, type, msg} ->
        error_type_msg(type, msg)

      {:ok, %Tds.Result{columns: lst_colum_names, rows: lst_data_rows, num_rows: row_count}} ->
        cond do
          lst_data_rows == nil ->
            {:ok, [], [], row_count}

          row_count == 0 ->
            {:error, :not_found, "NO se localizaron registros"}

          true ->
            record_set =
              lst_data_rows
              |> remove_raw_binary()
              |> to_struct(lst_colum_names, toStruct)

            {:ok, record_set, lst_colum_names, row_count}
        end
    end
  end

  def check_log(table) do
    # IO.inspect(table, label: "bitacora")
    msg_error =
      table
      |> Enum.filter(fn
        %{Alias: _proceso, Error: num_error, Mensaje: _msg, Operacion: _operacion} ->
          num_error > 0

        %{
          Alias: _proceso,
          Error: num_error,
          Mensaje: _msg,
          Operacion: _operacion,
          fecha_insert: _date
        } ->
          num_error > 0

        _item ->
          false
      end)
      |> Enum.reduce("", fn item, msg_error ->
        %{Alias: _proceso, Error: _num_error, Mensaje: msg, Operacion: _operacion} = item
        msg_error <> ",#{msg}"
      end)

    with one <- hd(table),
         %{Alias: _proceso, Error: _num_error, Mensaje: _msg, Operacion: _operacion} <- one do
      if msg_error == "" do
        {:ok, :empty, "Successfully processed"}
      else
        {:error, :sql_error, msg_error}
      end
    else
      _else ->
        {:ok, table}
    end
  end

  def struct_to_xml(attrs, tablename) when is_list(attrs) do
    data =
      attrs
      |> Enum.reduce("", fn map_item, acc_data ->
        {:ok, data} = struct_to_xml(map_item, tablename)
        acc_data <> data
      end)

    {:ok, data}
  end

  def struct_to_xml(attrs, tablename) do
    {:ok, data} = struct_to_xml(attrs)
    data = ~s(<#{tablename}>) <> data <> ~s(</#{tablename}>)
    {:ok, data}
  end

  def struct_to_xml(attrs) do
    data =
      attrs
      |> Map.to_list()
      |> Enum.reduce("", fn tupla, acc ->
        {k, v} = tupla
        acc <> ~s(<#{k}>#{v}</#{k}>)
      end)

    {:ok, data}
  end
end
