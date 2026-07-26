defmodule VenturiUiWeb.ChainController do
  use VenturiUiWeb, :controller

  alias VenturiUi.VenturiClient

  def index(conn, %{"parent_id" => parent_id}) when byte_size(parent_id) > 0 do
    lookup_form = to_form(%{"parent_id" => parent_id}, as: :lookup)
    link_form = to_form(%{}, as: :link)

    case VenturiClient.chain_references(parent_id) do
      {:ok, %{"references" => references}} ->
        render(conn, :index,
          lookup_form: lookup_form,
          link_form: link_form,
          parent_id: parent_id,
          references: references,
          error: nil
        )

      {:error, reason} ->
        render(conn, :index,
          lookup_form: lookup_form,
          link_form: link_form,
          parent_id: parent_id,
          references: nil,
          error: VenturiUiWeb.ApiError.format(reason)
        )
    end
  end

  def index(conn, _params) do
    render(conn, :index,
      lookup_form: to_form(%{"parent_id" => ""}, as: :lookup),
      link_form: to_form(%{}, as: :link),
      parent_id: nil,
      references: nil,
      error: nil
    )
  end

  def create_link(
        conn,
        %{
          "link" => %{
            "from_parent_id" => from_parent_id,
            "to_parent_id" => to_parent_id,
            "reference_type" => reference_type
          }
        }
      ) do
    case VenturiClient.link_chain(from_parent_id, to_parent_id, reference_type) do
      {:ok, _} ->
        conn
        |> put_flash(:info, "Linked #{from_parent_id} -> #{to_parent_id} (#{reference_type})")
        |> redirect(to: ~p"/chains?#{[parent_id: from_parent_id]}")

      {:error, reason} ->
        conn
        |> put_flash(:error, VenturiUiWeb.ApiError.format(reason))
        |> redirect(to: ~p"/chains")
    end
  end

  def create_link(conn, _params) do
    conn
    |> put_flash(:error, "from parent, to parent, and reference type are required")
    |> redirect(to: ~p"/chains")
  end
end
