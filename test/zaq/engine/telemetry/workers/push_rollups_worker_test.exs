defmodule Zaq.Engine.Telemetry.Workers.PushRollupsWorkerTest do
  use Zaq.DataCase, async: false

  alias Zaq.Engine.Telemetry
  alias Zaq.Engine.Telemetry.BenchmarkConnector.HTTP
  alias Zaq.Engine.Telemetry.Point
  alias Zaq.Engine.Telemetry.Rollup
  alias Zaq.Engine.Telemetry.Workers.AggregateRollupsWorker
  alias Zaq.Engine.Telemetry.Workers.PushRollupsWorker
  alias Zaq.Repo
  alias Zaq.System
  alias Zaq.System.Config

  setup do
    Repo.delete_all(Rollup)

    original = Application.get_env(:zaq, Telemetry, [])

    Application.put_env(
      :zaq,
      Telemetry,
      Keyword.merge(original, req_options: [plug: {Req.Test, HTTP}])
    )

    on_exit(fn -> Application.put_env(:zaq, Telemetry, original) end)

    :ok
  end

  test "perform/1 pushes local rollups and updates push cursor" do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    insert_rollup(now)

    assert {:ok, _} = System.set_config("telemetry.enabled", "true")
    assert {:ok, _} = System.set_config("telemetry.benchmark_opt_in", "true")

    Req.Test.stub(HTTP, fn conn ->
      assert conn.method == "POST"
      assert conn.request_path == "/api/v1/telemetry/rollups"

      {:ok, body, conn} = Plug.Conn.read_body(conn)
      decoded = Jason.decode!(body)

      assert is_map(decoded["org"])
      assert length(decoded["rollups"]) == 1

      Req.Test.json(conn, %{"ok" => true})
    end)

    assert :ok = PushRollupsWorker.perform(%{})
    assert %DateTime{} = Telemetry.get_cursor("telemetry.push_cursor")
  end

  test "perform/1 is a no-op when benchmark opt-in is disabled" do
    assert {:ok, _} = System.set_config("telemetry.enabled", "true")
    assert {:ok, _} = System.set_config("telemetry.benchmark_opt_in", "false")

    Req.Test.stub(HTTP, fn _conn ->
      flunk("remote API should not be called when benchmark opt-in is disabled")
    end)

    assert :ok = PushRollupsWorker.perform(%{})
  end

  test "perform/1 keeps identity-attributed LLM rollups local and advances the cursor" do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    insert_rollup(now,
      metric_key: "qa.llm.tokens.total",
      dimensions: %{"person_id" => "42", "model" => "gpt-4.1-mini"}
    )

    assert {:ok, _} = System.set_config("telemetry.enabled", "true")
    assert {:ok, _} = System.set_config("telemetry.benchmark_opt_in", "true")

    Req.Test.stub(HTTP, fn _conn ->
      flunk("identity-attributed rollups must not leave the local installation")
    end)

    assert :ok = PushRollupsWorker.perform(%{})
    assert %DateTime{} = Telemetry.get_cursor("telemetry.push_cursor")
  end

  describe "successive exports" do
    setup {Req.Test, :verify_on_exit!}

    setup do
      Repo.delete_all(Point)

      Repo.delete_all(
        from c in Config,
          where: c.key in ["telemetry.push_cursor", "telemetry.rollup_point_id_cursor"]
      )

      assert {:ok, _} = System.set_config("telemetry.enabled", "true")
      assert {:ok, _} = System.set_config("telemetry.benchmark_opt_in", "true")

      parent = self()

      Req.Test.stub(HTTP, fn conn ->
        assert conn.method == "POST"
        assert conn.request_path == "/api/v1/telemetry/rollups"
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        rollups = Jason.decode!(body)["rollups"]
        assert length(rollups) <= 1_000
        send(parent, {:pushed_rollups, rollups})
        Req.Test.json(conn, %{"ok" => true})
      end)

      :ok
    end

    test "export all rollups sharing a timestamp across batches" do
      aggregate_distinct_points(1_001)
      rollups = Repo.all(Rollup)

      # The timestamp collision is produced by the real aggregator, not by hand.
      assert length(Enum.uniq_by(rollups, & &1.updated_at)) == 1

      for _ <- 1..3, do: PushRollupsWorker.perform(%{})

      exported_models = Enum.map(exported_rollups(), & &1["dimensions"]["model"])
      expected_models = Enum.map(1..1_001, &"model-#{&1}")
      assert expected_models -- exported_models == []
      assert exported_models -- expected_models == []
    end

    test "include new rollups written by delayed aggregation" do
      # Model a rollup prepared earlier whose database write happens later.
      prepared_at = ~U[2026-10-29 12:01:00.000000Z]
      bucket_start = ~U[2026-10-29 12:00:00.000000Z]

      insert_rollup(DateTime.add(prepared_at, 1, :second),
        bucket_start: bucket_start,
        dimensions: %{"model" => "common"}
      )

      Req.Test.expect(HTTP, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        assert [%{"dimensions" => %{"model" => "common"}}] = Jason.decode!(body)["rollups"]

        # The HTTP request starts after batch selection. Insert the delayed row
        # before export succeeds, without scheduling concurrent workers.
        insert_rollup(prepared_at,
          bucket_start: bucket_start,
          dimensions: %{"model" => "target"},
          value_sum: 7.0,
          value_count: 1,
          value_min: 7.0,
          value_max: 7.0,
          last_value: 7.0
        )

        Req.Test.json(conn, %{"ok" => true})
      end)

      # A failed acknowledgment could otherwise make a replay export the target.
      assert :ok = PushRollupsWorker.perform(%{})

      # No further writes: later syncs must discover the delayed row.
      for _ <- 1..3, do: PushRollupsWorker.perform(%{})

      assert Enum.any?(exported_rollups(), fn rollup ->
               match?(
                 %{
                   "dimensions" => %{"model" => "target"},
                   "value_sum" => 7.0,
                   "value_count" => 1
                 },
                 rollup
               )
             end),
             "Target rollup was not exported."
    end
  end

  defp insert_rollup(updated_at, opts \\ []) do
    metric_key = Keyword.get(opts, :metric_key, "qa.answer.latency_ms")
    dimensions = Keyword.get(opts, :dimensions, %{})

    Repo.insert!(%Rollup{
      metric_key: metric_key,
      bucket_start: Keyword.get(opts, :bucket_start, DateTime.add(updated_at, -600, :second)),
      bucket_size: "10m",
      source: "local",
      dimensions: dimensions,
      dimension_key: Telemetry.dimension_key(dimensions),
      value_sum: Keyword.get(opts, :value_sum, 300.0),
      value_count: Keyword.get(opts, :value_count, 2),
      value_min: Keyword.get(opts, :value_min, 120.0),
      value_max: Keyword.get(opts, :value_max, 180.0),
      last_value: Keyword.get(opts, :last_value, 180.0),
      last_at: updated_at,
      inserted_at: updated_at,
      updated_at: updated_at
    })
  end

  defp aggregate_distinct_points(count) do
    now = ~U[2026-10-01 12:01:00.000000Z]

    points =
      for n <- 1..count do
        dimensions = %{"model" => "model-#{n}"}

        %{
          metric_key: "qa.export.count",
          occurred_at: now,
          value: 1.0,
          dimensions: dimensions,
          dimension_key: Telemetry.dimension_key(dimensions),
          source: "local",
          inserted_at: now
        }
      end

    assert {^count, nil} = Repo.insert_all(Point, points)
    assert :ok = AggregateRollupsWorker.perform(%{})
    assert Repo.aggregate(Rollup, :count) == count
  end

  defp exported_rollups do
    receive do
      {:pushed_rollups, rows} -> rows ++ exported_rollups()
    after
      0 -> []
    end
  end
end
