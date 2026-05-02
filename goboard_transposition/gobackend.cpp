#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <nlohmann/json.hpp>
#include "goboard.h"
#include <unordered_map>
#include <numeric>
#include <cstdio>
#include <vector>
#include <algorithm>

using json = nlohmann::json;
namespace py = pybind11;

// --- Extension for KataGo Analysis ---
class GoAnalyzer : public GoBoard {
    uint64_t hash_black_to_move_;
public:
    GoAnalyzer(int size) : GoBoard(size) {
        std::mt19937_64 rng(0x1337BEEF);
        hash_black_to_move_ = rng();
    }

    uint64_t getSitHash(Stone toMove) const {
        uint64_t h = hash();
        if (toMove == Stone::Black) h ^= hash_black_to_move_;
        return h;
    }

    void setupStone(int r, int c, Stone col) {
        if (onBoard(r, c) && at(r, c) == Stone::Empty) {
            this->play(r, c, col);
        }
    }

    std::pair<int, int> parseCoords(const std::string& s) const {
        if (s == "pass" || s == "PASS" || s == ".." || s == "tt") return {-1, -1};
        if (s.empty()) return {-1, -1};   // s[0] read below would be UB on empty
        // Cast to unsigned char before std::toupper: passing a negative `char`
        // (a high-bit byte under the default signed-char ABI on x86_64 Linux)
        // is UB per the C standard. Refusing non-ASCII-letter first chars
        // here also blocks crafted bytes from reaching the column arithmetic.
        unsigned char ch0 = static_cast<unsigned char>(s[0]);
        if (!((ch0 >= 'A' && ch0 <= 'Z') || (ch0 >= 'a' && ch0 <= 'z'))) {
            return {-1, -1};
        }
        char colChar = static_cast<char>(std::toupper(ch0));
        int col = colChar - 'A';
        if (colChar > 'I') col--;
        try {
            int rowNum = std::stoi(s.substr(1));
            int row = size() - rowNum;
            return {row, col};
        } catch (...) { return {-1, -1}; }
    }
};

// --- Partitioning Logic ---
std::string partition_pv(const std::string& req_str, const std::string& resp_str) {
    auto req = json::parse(req_str);
    auto resp = json::parse(resp_str);

    int size = req.value("boardXSize", 19);
#ifdef DEBUG
    fprintf(stderr, "[partition_pv] board size=%d\n", size);
#endif
    GoAnalyzer baseBoard(size);

    // 1. Initial Stones
    if (req.contains("initialStones")) {
        int nStones = (int)req["initialStones"].size();
#ifdef DEBUG
        fprintf(stderr, "[partition_pv] placing %d initial stones\n", nStones);
#endif
        for (auto& s : req["initialStones"]) {
            auto [r, c] = baseBoard.parseCoords(s[1].get<std::string>());
            if (r != -1) {
                Stone col = (s[0].get<std::string>() == "B" ? Stone::Black : Stone::White);
                baseBoard.setupStone(r, c, col);
            }
        }
    }

    // 2. Setup Starting Player
    Stone nextPla = Stone::Black;
    if (req.contains("initialPlayer")) {
        nextPla = (req["initialPlayer"].get<std::string>() == "B" ? Stone::Black : Stone::White);
    }

    // 3. Replay Move History
    int targetTurn = resp.value("turnNumber", 0);
#ifdef DEBUG
    fprintf(stderr, "[partition_pv] replaying to turnNumber=%d\n", targetTurn);
#endif
    if (req.contains("moves")) {
        int currentTurn = 0;
        for (auto& m : req["moves"]) {
            if (currentTurn >= targetTurn) break;
            Stone moveCol = (m[0].get<std::string>() == "B" ? Stone::Black : Stone::White);
            auto [r, c] = baseBoard.parseCoords(m[1].get<std::string>());
            if (r == -1) baseBoard.pass();
            else baseBoard.play(r, c, moveCol);
            nextPla = opponent(moveCol);
            currentTurn++;
        }
    }

    // 4. Union-Find on PVs
    auto& moveInfos = resp["moveInfos"];
    int k = moveInfos.size();
#ifdef DEBUG
    fprintf(stderr, "[partition_pv] running union-find over %d moveInfos\n", k);
#endif
    std::vector<int> parent(k);
    std::iota(parent.begin(), parent.end(), 0);
    // Iterative two-walk path compression. The previous recursive lambda
    // could stack-overflow on long parent chains assembled across many
    // moveInfos with overlapping transpositions; the iterative form bounds
    // stack use to O(1) regardless of chain length.
    auto find = [&](int i) -> int {
        int root = i;
        while (parent[root] != root) root = parent[root];
        int cur = i;
        while (parent[cur] != root) {
            int next = parent[cur];
            parent[cur] = root;
            cur = next;
        }
        return root;
    };

    std::unordered_map<uint64_t, int> seenHashes;
    Stone rootPla = (resp["rootInfo"]["currentPlayer"].get<std::string>() == "B" ? Stone::Black : Stone::White);

    int mergeCount = 0;
    for (int i = 0; i < k; ++i) {
        if (!moveInfos[i].contains("pv") || moveInfos[i]["pv"].empty()) continue;

        GoAnalyzer testBoard = baseBoard;
        Stone pla = rootPla;

        for (const std::string& moveStr : moveInfos[i]["pv"]) {
            auto [r, c] = testBoard.parseCoords(moveStr);
            if (r == -1) testBoard.pass();
            else testBoard.play(r, c, pla);
            pla = opponent(pla);

            uint64_t h = testBoard.getSitHash(pla);
            if (seenHashes.count(h)) {
                int rootA = find(i);
                int rootB = find(seenHashes[h]);
                if (rootA != rootB) {
#ifdef DEBUG
                    fprintf(stderr, "[partition_pv] merge: moveInfo[%d] -> moveInfo[%d] via hash 0x%016llx at move \"%s\"\n",
                            i, seenHashes[h], (unsigned long long)h, moveStr.c_str());
#endif
                    parent[rootA] = rootB;
                    ++mergeCount;
                }
                break;
            } else {
                seenHashes[h] = i;
            }
        }
    }
#ifdef DEBUG
    fprintf(stderr, "[partition_pv] total merges=%d, distinct hashes seen=%zu\n", mergeCount, seenHashes.size());
#endif

    // 5. Compute total visits per cluster (after union-find is complete)
    std::unordered_map<int, long long> clusterVisits;
    for (int i = 0; i < k; ++i) {
        int root = find(i);
        long long visits = moveInfos[i].value("visits", 0LL);
        clusterVisits[root] += visits;
    }
#ifdef DEBUG
    fprintf(stderr, "[partition_pv] computed visit sums for %zu clusters\n", clusterVisits.size());
#endif

    // 6. Sort clusters by decreasing total visits
    std::vector<std::pair<long long, int>> clusterList;
    clusterList.reserve(clusterVisits.size());
    for (const auto& p : clusterVisits) {
        clusterList.emplace_back(p.second, p.first);  // (total_visits, root)
    }
    std::sort(clusterList.begin(), clusterList.end(),
              [](const auto& a, const auto& b) {
                  return a.first > b.first;   // descending visits
              });

    // 7. Assign new clusterIds in decreasing-visit order
    std::unordered_map<int, int> canonToId;
    int nextId = 0;
    for (const auto& p : clusterList) {
        canonToId[p.second] = nextId++;
    }
#ifdef DEBUG
    fprintf(stderr, "[partition_pv] reassigned %d clusters ordered by decreasing total visits\n", nextId);
#endif

    // 8. Write the new clusterId into every moveInfo
    for (int i = 0; i < k; ++i) {
        int root = find(i);
        moveInfos[i]["clusterId"] = canonToId[root];
    }

    return resp.dump();
}

// MODULE_NAME is injected by the build system via -DMODULE_NAME=<name>.
// Both this symbol and the output filename must agree; meson handles that.
PYBIND11_MODULE(MODULE_NAME, m) {
    m.def("partition_pv", &partition_pv,
          "Partition KataGo PVs into transposition equivalence classes");
}
