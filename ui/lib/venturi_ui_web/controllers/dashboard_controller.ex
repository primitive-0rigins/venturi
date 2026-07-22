defmodule VenturiUiWeb.DashboardController do
  use VenturiUiWeb, :controller

  alias VenturiUi.VenturiClient

  def home(conn, _params) do
    case VenturiClient.health() do
      {:ok, health} ->
        render(conn, :home, health: health, error: nil)

      {:error, reason} ->
        render(conn, :home, health: nil, error: VenturiUiWeb.ApiError.format(reason))
    end
  end
end
