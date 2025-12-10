#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include "boruvka.hxx"
#include <gunrock/algorithms/algorithms.hxx>
#include <gunrock/algorithms/mst.hxx>

int main(int argc, char **argv) {
  using namespace gunrock;
  using vertex_t = int;
  using edge_t = int;
  using weight_t = float;
  using csr_t =
      format::csr_t<memory_space_t::device, vertex_t, edge_t, weight_t>;

  if (argc < 2) {
    std::fprintf(stderr, "Usage: %s <symmetric_matrix_market_file>\n", argv[0]);
    return 1;
  }
  std::string filename = argv[1];

  io::matrix_market_t<vertex_t, edge_t, weight_t> mm;
  auto [props, coo] = mm.load(filename);
  if (!props.symmetric) {
    std::fprintf(stderr, "Error: graph must be symmetric (undirected)\n");
    return 2;
  }

  csr_t csr;
  csr.from_coo(coo);
  auto G = graph::build<memory_space_t::device>(props, csr);

  auto context = std::shared_ptr<gunrock::gcuda::multi_context_t>(
      new gunrock::gcuda::multi_context_t(0));

  weight_t w_frontier = 0;
  auto res = boruvka::run(G, &w_frontier, context, true);
  std::printf("\nFrontier Boruvka MST Weight: %.6f (iterations=%d)\n",
              (double)res.mst_weight, res.iterations);
  std::printf("Handmade MST GPU time: %.3f ms\n", res.gpu_milliseconds);
  std::printf("Handmade MST Wall time: %.3f ms\n", res.wall_milliseconds);

  thrust::device_vector<weight_t> d_w(1);
  float mst_ms = gunrock::mst::run(G, d_w.data().get(), context);
  thrust::host_vector<weight_t> h_w = d_w;
  std::printf("\nGunrock MST Weight: %.6f (reported GPU time=%.3f ms)\n",
              (double)h_w[0], mst_ms);

  if (std::abs(h_w[0] - w_frontier) > 1e-3)
    std::fprintf(stderr, "Mismatch: frontier=%.6f gunrock=%.6f\n",
                 (double)w_frontier, (double)h_w[0]);

  return 0;
}
