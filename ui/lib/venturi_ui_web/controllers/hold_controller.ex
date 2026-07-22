defmodule VenturiUiWeb.HoldController do
  use VenturiUiWeb, :controller

  alias VenturiUi.VenturiClient

  def index(conn, _params) do
    render(conn, :index,
      hold_form: to_form(%{}, as: :hold),
      release_form: to_form(%{}, as: :release)
    )
  end

  def create(conn, %{"hold" => %{"parent_id" => parent_id, "reason" => reason}}) do
    case VenturiClient.place_hold(parent_id, reason) do
      {:ok, _} ->
        conn
        |> put_flash(:info, "Legal hold placed on #{parent_id}")
        |> redirect(to: ~p"/holds")

      {:error, reason} ->
        conn
        |> put_flash(:error, VenturiUiWeb.ApiError.format(reason))
        |> redirect(to: ~p"/holds")
    end
  end

  def delete(conn, %{"release" => %{"parent_id" => parent_id}}) do
    case VenturiClient.release_hold(parent_id) do
      {:ok, _} ->
        conn
        |> put_flash(:info, "Legal hold released on #{parent_id}")
        |> redirect(to: ~p"/holds")

      {:error, reason} ->
        conn
        |> put_flash(:error, VenturiUiWeb.ApiError.format(reason))
        |> redirect(to: ~p"/holds")
    end
  end
end
