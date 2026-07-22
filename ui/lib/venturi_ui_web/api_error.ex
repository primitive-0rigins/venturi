defmodule VenturiUiWeb.ApiError do
  @moduledoc "Formats VenturiUi.VenturiClient error tuples for display in flash/page messages."

  def format({:api_error, status, body}), do: "API returned #{status}: #{inspect(body)}"
  def format({:transport_error, reason}), do: "Could not reach Venturi: #{inspect(reason)}"
end
