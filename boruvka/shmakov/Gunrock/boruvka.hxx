#pragma once

#include <gunrock/algorithms/algorithms.hxx>
#include <gunrock/cuda/context.hxx>
#include <gunrock/framework/frontier/frontier.hxx>
#include <gunrock/framework/operators/filter/filter.hxx>
#include <gunrock/framework/operators/for/for.hxx> // parallel_for operator
#include <gunrock/util/math.hxx>

#include <thrust/copy.h>
#include <thrust/device_vector.h>
#include <thrust/fill.h>
#include <thrust/host_vector.h>
#include <thrust/sequence.h>

#include <chrono>
#include <cstdio>
#include <cuda_runtime.h>
#include <limits>
#include <memory>

namespace boruvka {

template <typename graph_t> struct state_t {
  using vertex_t = typename graph_t::vertex_type;
  using edge_t = typename graph_t::edge_type;
  using weight_t = typename graph_t::weight_type;

  int n_vertices = 0;
  int n_edges = 0;

  thrust::device_vector<vertex_t> roots;
  thrust::device_vector<vertex_t> new_roots;
  thrust::device_vector<weight_t> min_weights;
  thrust::device_vector<edge_t> min_edges;
  thrust::device_vector<int> super_vertices;
  thrust::device_vector<bool> not_decremented;

  void init(graph_t &G, gunrock::gcuda::multi_context_t &ctx) {
    n_vertices = G.get_number_of_vertices();
    n_edges = G.get_number_of_edges();
    roots.resize(n_vertices);
    new_roots.resize(n_vertices);
    min_weights.resize(n_vertices);
    min_edges.resize(n_vertices);
    super_vertices.resize(1);
    not_decremented.resize(1);
    reset(ctx);
  }

  void reset(gunrock::gcuda::multi_context_t &ctx) {
    auto exec = ctx.get_context(0)->execution_policy();
    thrust::fill(exec, min_weights.begin(), min_weights.end(),
                 std::numeric_limits<weight_t>::max());
    thrust::fill(exec, min_edges.begin(), min_edges.end(),
                 std::numeric_limits<edge_t>::max());
    thrust::fill(exec, super_vertices.begin(), super_vertices.end(),
                 n_vertices);
    thrust::fill(exec, not_decremented.begin(), not_decremented.end(), false);
    thrust::sequence(exec, roots.begin(), roots.end(), 0);
    thrust::sequence(exec, new_roots.begin(), new_roots.end(), 0);
  }
};

struct result_t {
  double mst_weight = 0.0;
  int iterations = 0;
  float gpu_milliseconds = 0.0f;  // CUDA events timing for MST loop
  double wall_milliseconds = 0.0; // Wall-clock timing for MST loop
};

template <typename graph_t>
result_t run(graph_t &G, typename graph_t::weight_type *mst_weight_out,
             std::shared_ptr<gunrock::gcuda::multi_context_t> context =
                 std::shared_ptr<gunrock::gcuda::multi_context_t>(
                     new gunrock::gcuda::multi_context_t(0)),
             bool verbose = true, int max_iters = 10000) {

  using vertex_t = typename graph_t::vertex_type;
  using edge_t = typename graph_t::edge_type;
  using weight_t = typename graph_t::weight_type;

  result_t R;
  if (G.get_number_of_vertices() == 0) {
    if (mst_weight_out)
      *mst_weight_out = 0;
    return R;
  }

  auto &ctx = *context;
  auto stream = ctx.get_context(0)->stream();

  state_t<graph_t> S;
  S.init(G, ctx);

  thrust::device_vector<weight_t> d_mst(1);
  {
    auto exec = ctx.get_context(0)->execution_policy();
    thrust::fill(exec, d_mst.begin(), d_mst.end(), (weight_t)0);
  }

  using edge_frontier_t = gunrock::frontier::frontier_t<
      vertex_t, edge_t, gunrock::frontier::frontier_kind_t::edge_frontier>;

  edge_frontier_t in_edges;
  edge_frontier_t filtered;

  in_edges.reserve(S.n_edges);
  in_edges.set_number_of_elements(S.n_edges);
  in_edges.sequence((edge_t)0, S.n_edges, stream);
  ctx.get_context(0)->synchronize();

  auto roots_ptr = S.roots.data().get();
  auto new_roots_ptr = S.new_roots.data().get();
  auto min_weights_ptr = S.min_weights.data().get();
  auto min_edges_ptr = S.min_edges.data().get();
  auto mst_ptr = d_mst.data().get();
  auto super_ptr = S.super_vertices.data().get();
  auto not_dec_ptr = S.not_decremented.data().get();

  // Timing setup (CUDA events on Gunrock's stream + wall clock)
  cudaEvent_t ev_start, ev_stop;
  cudaEventCreate(&ev_start);
  cudaEventCreate(&ev_stop);
  auto wall_start = std::chrono::steady_clock::now();
  cudaEventRecord(ev_start, stream);

  int iter = 0;
  while (iter < max_iters) {
    ++iter;

    {
      auto exec = ctx.get_context(0)->execution_policy();
      thrust::fill(exec, S.min_weights.begin(), S.min_weights.end(),
                   std::numeric_limits<weight_t>::max());
      thrust::fill(exec, S.min_edges.begin(), S.min_edges.end(),
                   std::numeric_limits<edge_t>::max());
      thrust::fill(exec, S.not_decremented.begin(), S.not_decremented.end(),
                   true);
    }

    auto filter_lambda = [roots_ptr, min_weights_ptr, G] __host__ __device__(
                             edge_t const &e) -> bool {
      auto src = G.get_source_vertex(e);
      auto dst = G.get_destination_vertex(e);
      if (src == dst)
        return false;
      if (src < dst && roots_ptr[src] != roots_ptr[dst]) {
        auto w = G.get_edge_weight(e);
        auto old1 =
            gunrock::math::atomic::min(&(min_weights_ptr[roots_ptr[src]]), w);
        auto old2 =
            gunrock::math::atomic::min(&(min_weights_ptr[roots_ptr[dst]]), w);
        return (w <= old1) || (w <= old2);
      }
      return false;
    };

    gunrock::operators::filter::execute<
        gunrock::operators::filter_algorithm_t::remove>(
        G, filter_lambda, &in_edges, &filtered, ctx);

    auto min_edge_lambda =
        [roots_ptr, min_weights_ptr, min_edges_ptr, G] __host__ __device__(
            edge_t const &e) -> void {
      auto src = G.get_source_vertex(e);
      auto dst = G.get_destination_vertex(e);
      if (src < dst && roots_ptr[src] != roots_ptr[dst]) {
        auto w = G.get_edge_weight(e);
        if (w == min_weights_ptr[roots_ptr[src]])
          gunrock::math::atomic::min(&(min_edges_ptr[roots_ptr[src]]), e);
        if (w == min_weights_ptr[roots_ptr[dst]])
          gunrock::math::atomic::min(&(min_edges_ptr[roots_ptr[dst]]), e);
      }
    };

    gunrock::operators::parallel_for::execute<
        gunrock::operators::parallel_for_each_t::element>(filtered,
                                                          min_edge_lambda, ctx);

    auto add_lambda = [roots_ptr, new_roots_ptr, min_weights_ptr, min_edges_ptr,
                       mst_ptr, super_ptr, not_dec_ptr, G] __host__
                      __device__(vertex_t const &v) -> void {
      if (roots_ptr[v] != v)
        return;
      auto w = min_weights_ptr[v];
      if (w == std::numeric_limits<weight_t>::max())
        return;
      auto e = min_edges_ptr[v];
      if (e == std::numeric_limits<edge_t>::max())
        return;

      auto src = G.get_source_vertex(e);
      auto dst = G.get_destination_vertex(e);
      auto we = G.get_edge_weight(e);

      if (roots_ptr[src] != v) {
        auto tmp = src;
        src = dst;
        dst = tmp;
      }

      if (src < dst || min_edges_ptr[roots_ptr[dst]] != e) {
        not_dec_ptr[0] = false;
        gunrock::math::atomic::add(&mst_ptr[0], we);
        gunrock::math::atomic::add(&super_ptr[0], -1);
        gunrock::math::atomic::exch(&new_roots_ptr[v], new_roots_ptr[dst]);
      }
    };

    gunrock::operators::parallel_for::execute<
        gunrock::operators::parallel_for_each_t::vertex>(G, add_lambda, ctx);

    ctx.get_context(0)->synchronize();

    auto jump_lambda = [new_roots_ptr] __host__ __device__(vertex_t const &v)
        -> void {
          vertex_t u = new_roots_ptr[v];
          while (new_roots_ptr[u] != u) {
            u = new_roots_ptr[u];
          }
          new_roots_ptr[v] = u;
        };

    gunrock::operators::parallel_for::execute<
        gunrock::operators::parallel_for_each_t::vertex>(G, jump_lambda, ctx);

    {
      auto exec = ctx.get_context(0)->execution_policy();
      thrust::copy(exec, S.new_roots.begin(), S.new_roots.end(),
                   S.roots.begin());
    }

    {
      thrust::host_vector<int> h_super = S.super_vertices;
      if (verbose) {
        float partial_w = 0.0f;
        cudaMemcpy(&partial_w, d_mst.data().get(), sizeof(float),
                   cudaMemcpyDeviceToHost);
        printf("Iter %d: super_vertices=%d, partial_weight=%.6f\n", iter,
               h_super[0], (double)partial_w);
      }
      if (h_super[0] == 1)
        break;
    }
  }

  cudaEventRecord(ev_stop, stream);
  cudaEventSynchronize(ev_stop);
  float gpu_ms = 0.0f;
  cudaEventElapsedTime(&gpu_ms, ev_start, ev_stop);
  cudaEventDestroy(ev_start);
  cudaEventDestroy(ev_stop);

  auto wall_end = std::chrono::steady_clock::now();
  double wall_ms =
      std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
          wall_end - wall_start)
          .count();

  float final_w = 0.0f;
  cudaMemcpy(&final_w, d_mst.data().get(), sizeof(float),
             cudaMemcpyDeviceToHost);
  if (mst_weight_out)
    *mst_weight_out = final_w;

  R.mst_weight = final_w;
  R.iterations = iter;
  R.gpu_milliseconds = gpu_ms;
  R.wall_milliseconds = wall_ms;
  return R;
}

} // namespace boruvka
