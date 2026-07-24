defmodule VenturiUi.VenturiClient do
  @moduledoc """
  Thin HTTP client over the Venturi API. The operator dashboard is read/light-write
  only — it calls existing endpoints and adds no new server-side behavior.
  """

  defp config, do: Application.get_env(:venturi_ui, :venturi_api, [])

  defp base_url, do: Keyword.fetch!(config(), :base_url)

  defp req do
    opts = [base_url: base_url(), retry: false]

    opts =
      case Keyword.get(config(), :api_key) do
        nil -> opts
        key -> Keyword.put(opts, :auth, {:bearer, key})
      end

    opts =
      case Keyword.get(config(), :plug) do
        nil -> opts
        plug -> Keyword.put(opts, :plug, plug)
      end

    Req.new(opts)
  end

  @doc "GET /health"
  def health do
    req() |> Req.get(url: "/health") |> respond()
  end

  @doc "GET /audit/:retrieval_audit_id"
  def audit(retrieval_audit_id) do
    req() |> Req.get(url: "/audit/#{encode_segment(retrieval_audit_id)}") |> respond()
  end

  @doc "GET /chain/references/:parent_id"
  def chain_references(parent_id) do
    req() |> Req.get(url: "/chain/references/#{encode_segment(parent_id)}") |> respond()
  end

  @doc "POST /chain/link"
  def link_chain(from_parent_id, to_parent_id, reference_type) do
    body = %{
      from_parent_id: from_parent_id,
      to_parent_id: to_parent_id,
      reference_type: reference_type
    }

    req() |> Req.post(url: "/chain/link", json: body) |> respond()
  end

  @doc "POST /hold"
  def place_hold(parent_id, reason) do
    req()
    |> Req.post(url: "/hold", json: %{parent_id: parent_id, reason: reason})
    |> respond()
  end

  @doc "DELETE /hold/:parent_id"
  def release_hold(parent_id) do
    req() |> Req.delete(url: "/hold/#{encode_segment(parent_id)}") |> respond()
  end

  # `URI.encode/1`'s default predicate leaves reserved characters like `/`, `?`,
  # and `#` untouched, so a ref containing one would corrupt the request path.
  defp encode_segment(id), do: URI.encode(id, &URI.char_unreserved?/1)

  defp respond({:ok, %Req.Response{status: status, body: body}}) when status in 200..299 do
    {:ok, body}
  end

  defp respond({:ok, %Req.Response{status: status, body: body}}) do
    {:error, {:api_error, status, body}}
  end

  defp respond({:error, reason}) do
    {:error, {:transport_error, reason}}
  end
end
