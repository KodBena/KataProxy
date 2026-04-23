#pragma once
// goboard.h – Go board with simple ko, captures, and Zobrist hashing (C++20)

#include <array>
#include <cassert>
#include <cstdint>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// ======================================================================
//  Stone type
// ======================================================================
enum class Stone : std::uint8_t { Empty = 0, Black = 1, White = 2 };

constexpr Stone opponent(Stone s) noexcept {
    return s == Stone::Black ? Stone::White : Stone::Black;
}

// ======================================================================
//  GoBoard
// ======================================================================
class GoBoard {
public:
    // ---- Construction ----------------------------------------------------

    /// Create an empty size×size board.  `zobristSeed` seeds the random
    /// table used for incremental Zobrist hashing.
    explicit GoBoard(int size = 19,
                     std::uint64_t zobristSeed = 0xDEAD'BEEF'CAFE'1234ULL)
        : size_{size}, board_(size * size, Stone::Empty)
    {
        assert(size >= 1);
        initZobrist(zobristSeed);
    }

    // ---- Accessors -------------------------------------------------------

    int           size()  const noexcept { return size_; }
    std::uint64_t hash()  const noexcept { return hash_; }

    /// Stone at (row, col), both 0-indexed.
    Stone at(int row, int col) const {
        assert(onBoard(row, col));
        return board_[pos(row, col)];
    }

    /// Number of opponent stones captured **by** `color`.
    int captures(Stone color) const noexcept {
        return color == Stone::Black ? blackCaps_ : whiteCaps_;
    }

    /// If a simple-ko restriction is active, returns (row, col) of the
    /// forbidden point together with the color that may NOT play there.
    std::optional<std::pair<int, int>> koPoint() const {
        if (koPos_ < 0) return std::nullopt;
        return {{koPos_ / size_, koPos_ % size_}};
    }
    Stone koColor() const noexcept { return koCol_; }

    // ---- Legality --------------------------------------------------------

    /// True if `color` may legally play at (row, col).
    /// Checks: on-board, empty, not ko-forbidden, not suicide.
    bool isLegal(int row, int col, Stone color) const;

    // ---- Mutators --------------------------------------------------------

    /// Place a stone.  Returns `true` if the move was legal (and executed),
    /// `false` if it was illegal (board unchanged).
    bool play(int row, int col, Stone color);

    /// Pass.  Clears any ko restriction.
    void pass() noexcept { koPos_ = -1; koCol_ = Stone::Empty; }

    // ---- Display ---------------------------------------------------------

    std::string toString() const;

    // ======================================================================
protected:
    // ---- Data ------------------------------------------------------------
    int                size_;
    std::vector<Stone> board_;
    int                blackCaps_ = 0;   // prisoners taken by Black
    int                whiteCaps_ = 0;   // prisoners taken by White
    std::uint64_t      hash_      = 0;

    int   koPos_ = -1;                   // forbidden position (-1 = none)
    Stone koCol_ = Stone::Empty;         // color that may NOT play there

    // Zobrist random table: [boardPosition][0=Black, 1=White]
    std::vector<std::array<std::uint64_t, 2>> ztab_;

    // ---- Helpers ---------------------------------------------------------
    int  pos(int r, int c) const noexcept { return r * size_ + c; }
    bool onBoard(int r, int c) const noexcept {
        return static_cast<unsigned>(r) < static_cast<unsigned>(size_)
            && static_cast<unsigned>(c) < static_cast<unsigned>(size_);
    }

    /// Invoke `fn(neighbourIndex)` for every orthogonal neighbour of `p`.
    template <typename F>
    void forNeighbours(int p, F&& fn) const {
        const int r = p / size_, c = p % size_;
        if (r > 0)          fn(p - size_);
        if (r < size_ - 1)  fn(p + size_);
        if (c > 0)          fn(p - 1);
        if (c < size_ - 1)  fn(p + 1);
    }

    // A connected group of same-coloured stones and its liberty count.
    struct Group {
        std::vector<int> stones;
        int              liberties = 0;
    };

    /// BFS from `start`; returns every stone in the connected group and
    /// the number of unique liberties.
    Group findGroup(int start) const {
        Group g;
        const Stone col = board_[start];
        if (col == Stone::Empty) return g;

        const int N = size_ * size_;
        std::vector<bool> visited(N, false);
        std::vector<bool> libSeen(N, false);

        std::vector<int> stack;
        stack.push_back(start);
        visited[start] = true;

        while (!stack.empty()) {
            const int cur = stack.back();
            stack.pop_back();
            g.stones.push_back(cur);

            forNeighbours(cur, [&](int nb) {
                if (board_[nb] == Stone::Empty) {
                    if (!libSeen[nb]) { libSeen[nb] = true; ++g.liberties; }
                } else if (board_[nb] == col && !visited[nb]) {
                    visited[nb] = true;
                    stack.push_back(nb);
                }
            });
        }
        return g;
    }

    // Place / remove helpers (maintain Zobrist hash).
    void placeStone(int p, Stone col) {
        assert(board_[p] == Stone::Empty && col != Stone::Empty);
        board_[p] = col;
        hash_ ^= ztab_[p][col == Stone::Black ? 0 : 1];
    }
    void clearStone(int p) {
        const Stone col = board_[p];
        assert(col != Stone::Empty);
        hash_ ^= ztab_[p][col == Stone::Black ? 0 : 1];
        board_[p] = Stone::Empty;
    }
    int removeGroup(const std::vector<int>& stones) {
        for (int p : stones) clearStone(p);
        return static_cast<int>(stones.size());
    }

    void initZobrist(std::uint64_t seed) {
        ztab_.resize(size_ * size_);
        std::mt19937_64 rng(seed);
        for (auto& entry : ztab_) { entry[0] = rng(); entry[1] = rng(); }
    }
};

// ======================================================================
//  Out-of-line definitions (still header-only thanks to `inline`)
// ======================================================================

inline bool GoBoard::isLegal(int row, int col, Stone color) const {
    if (color == Stone::Empty || !onBoard(row, col)) return false;
    const int p = pos(row, col);
    if (board_[p] != Stone::Empty) return false;
    if (koPos_ == p && koCol_ == color) return false;   // ko

    const Stone opp = opponent(color);

    // (1) Immediate liberty – any adjacent empty intersection.
    bool liberty = false;
    forNeighbours(p, [&](int nb) {
        if (board_[nb] == Stone::Empty) liberty = true;
    });
    if (liberty) return true;

    // (2) Capture – any adjacent opponent group with exactly 1 liberty
    //     (that liberty must be `p`, so placing here kills it).
    bool capturesAny = false;
    forNeighbours(p, [&](int nb) {
        if (!capturesAny && board_[nb] == opp)
            if (findGroup(nb).liberties == 1) capturesAny = true;
    });
    if (capturesAny) return true;

    // (3) Friendly connection – an adjacent own group that has at least
    //     one liberty *besides* `p`, so the merged group survives.
    bool alive = false;
    forNeighbours(p, [&](int nb) {
        if (!alive && board_[nb] == color)
            if (findGroup(nb).liberties > 1) alive = true;
    });
    return alive;      // false → suicide (illegal)
}

inline bool GoBoard::play(int row, int col, Stone color) {
    assert(color != Stone::Empty);
    if (!isLegal(row, col, color)) return false;

    const int p   = pos(row, col);
    const Stone opp = opponent(color);

    placeStone(p, color);

    // --- Capture dead opponent groups adjacent to the placed stone --------
    int totalCaptured  = 0;
    int singleCapAt    = -1;          // position of a lone captured stone

    forNeighbours(p, [&](int nb) {
        // After an earlier removal in this loop the cell may already be
        // empty; that is fine – board_[nb] != opp will skip it.
        if (board_[nb] != opp) return;
        Group g = findGroup(nb);
        if (g.liberties == 0) {
            totalCaptured += static_cast<int>(g.stones.size());
            if (g.stones.size() == 1) singleCapAt = g.stones[0];
            removeGroup(g.stones);
        }
    });

    (color == Stone::Black ? blackCaps_ : whiteCaps_) += totalCaptured;

    // --- Simple ko detection ---------------------------------------------
    // A ko exists when exactly one stone was captured and the capturing
    // stone itself is a single-stone group with exactly one liberty
    // (the just-vacated point).  The opponent may not immediately recapture.
    koPos_ = -1;
    koCol_ = Stone::Empty;

    if (totalCaptured == 1) {
        Group cg = findGroup(p);
        if (static_cast<int>(cg.stones.size()) == 1 && cg.liberties == 1) {
            koPos_ = singleCapAt;
            koCol_ = opp;
        }
    }

    return true;
}

inline std::string GoBoard::toString() const {
    static constexpr char glyph[] = ".XO";
    std::ostringstream os;

    // Column letters (skip 'I' per Go convention; fall back for >25 cols)
    auto colLabel = [](int c) -> char {
        char ch = static_cast<char>('A' + c);
        if (ch >= 'I') ++ch;           // skip I
        return ch <= 'Z' ? ch : '?';
    };

    // Top column header
    os << "   ";
    for (int c = 0; c < size_; ++c) os << colLabel(c) << ' ';
    os << '\n';

    for (int r = 0; r < size_; ++r) {
        const int rowNum = size_ - r;
        if (rowNum < 10) os << ' ';
        os << rowNum << ' ';
        for (int c = 0; c < size_; ++c)
            os << glyph[static_cast<int>(board_[pos(r, c)])] << ' ';
        os << rowNum << '\n';
    }

    // Bottom column header
    os << "   ";
    for (int c = 0; c < size_; ++c) os << colLabel(c) << ' ';
    os << '\n';

    os << "Black captures: " << blackCaps_
       << "  |  White captures: " << whiteCaps_ << '\n';
    os << "Zobrist hash: 0x" << std::hex << hash_ << std::dec << '\n';
    if (koPos_ >= 0)
        os << "Ko: " << (koCol_ == Stone::Black ? "Black" : "White")
           << " may not play at ("
           << koPos_ / size_ << ',' << koPos_ % size_ << ")\n";
    return os.str();
}

