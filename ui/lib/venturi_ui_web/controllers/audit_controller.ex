defmodule VenturiUiWeb.AuditController do
  use VenturiUiWeb, :controller

  alias VenturiUi.VenturiClient

  def new(conn, %{"retrieval_audit_id" => id}) when byte_size(id) > 0 do
    form = to_form(%{"retrieval_audit_id" => id}, as: :lookup)

    case VenturiClient.audit(id) do
      {:ok, %{"proof" => proof}} ->
        render(conn, :new, form: form, proof: proof, error: nil)

      {:error, reason} ->
        render(conn, :new, form: form, proof: nil, error: VenturiUiWeb.ApiError.format(reason))
    end
  end

  def new(conn, _params) do
    form = to_form(%{"retrieval_audit_id" => ""}, as: :lookup)
    render(conn, :new, form: form, proof: nil, error: nil)
  end
end
