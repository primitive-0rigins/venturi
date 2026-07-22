defmodule VenturiUiWeb.AuditControllerTest do
  use VenturiUiWeb.ConnCase, async: false

  test "GET /audit renders an empty lookup form", %{conn: conn} do
    conn = get(conn, ~p"/audit")
    assert html_response(conn, 200) =~ "Retrieval Audit Lookup"
  end

  test "GET /audit?retrieval_audit_id=... renders the proof", %{conn: conn} do
    Req.Test.stub(VenturiUi.VenturiClient, fn conn ->
      Req.Test.json(conn, %{
        "proof" => %{
          "retrieval_audit_id" => "abc-123",
          "actor_id" => "agent-42",
          "mode" => "context",
          "query" => "patient chest pain",
          "filters_applied" => %{"domain" => "medical"},
          "candidate_count" => 5,
          "selected_orb_ids" => ["orb-1"],
          "selected_parent_ids" => ["parent-1"],
          "key_ids_used" => [],
          "embedding_model_version" => "nomic-embed-text",
          "chain_complete" => true,
          "retrieval_timestamp" => "1753142400Z"
        }
      })
    end)

    conn = get(conn, ~p"/audit?retrieval_audit_id=abc-123")

    body = html_response(conn, 200)
    assert body =~ "abc-123"
    assert body =~ "agent-42"
    assert body =~ "patient chest pain"
  end

  test "GET /audit?retrieval_audit_id=... surfaces a not-found error", %{conn: conn} do
    Req.Test.stub(VenturiUi.VenturiClient, fn conn ->
      conn
      |> Plug.Conn.put_status(404)
      |> Req.Test.json(%{"error" => "retrieval proof not found"})
    end)

    conn = get(conn, ~p"/audit?retrieval_audit_id=missing")

    assert html_response(conn, 200) =~ "API returned 404"
  end
end
