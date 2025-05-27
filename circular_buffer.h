#ifndef CIRCULAR_BUFFER_H
#define CIRCULAR_BUFFER_H

#include <cstddef>
#include <optional>
#include <utility>

#include <mutex>

/**
 * @brief A thread-safe circular buffer that supports head pop and tail insert, steal, as well as unstealable "stickied" elements; head pop always takes precedence over tail steal
 * @warning T must be default constructible and move-assignable
 */
template <typename T>
class Extrema_Circular_Buffer {
private:

    using Elem = std::pair<T, bool>;

    size_t capacity;
    #define CAPACITY_ACTUAL (capacity + 1)

    Elem* data;

    size_t idx_head, idx_next_empty;

    // mutex for the head element and index; controls whether or not we can pop
    mutable std::mutex mut_head;

    // mutex for the tail lement and index; controls whether or not we can insert and steal
    mutable std::mutex mut_tail;

    size_t idx_next(size_t idx) const {
        return (idx + 1) % CAPACITY_ACTUAL;
    }

    size_t idx_prev(size_t idx) const {
        return (idx + CAPACITY_ACTUAL - 1) % CAPACITY_ACTUAL;
    }

    size_t size_nonlocking() const {
        return (idx_next_empty - idx_head + CAPACITY_ACTUAL) % CAPACITY_ACTUAL;
    }

    bool empty_nonlocking() const {
        return idx_head == idx_next_empty;
    }

    bool full_nonlocking() const {
        return (idx_next_empty + 1) % CAPACITY_ACTUAL == idx_head;
    }

public:

    Extrema_Circular_Buffer(size_t capacity) :
        capacity{capacity},
        data{new Elem[CAPACITY_ACTUAL]},
        idx_head{0},
        idx_next_empty{0}
    {}

    Extrema_Circular_Buffer(const Extrema_Circular_Buffer& other) :
        capacity{other.capacity},
        data{new Elem[CAPACITY_ACTUAL]},
        idx_head{other.idx_head},
        idx_next_empty{other.idx_next_empty}
    {
        for(size_t i = 0; i < CAPACITY_ACTUAL; ++i) {
            data[i] = other.data[i];
        }
    }

    Extrema_Circular_Buffer(Extrema_Circular_Buffer&& other) :
        capacity{other.capacity},
        data{other.data},
        idx_head{other.idx_head},
        idx_next_empty{other.idx_next_empty},
    {
        other.data = nullptr; // prevent deletion in destructor
    }

    Extrema_Circular_Buffer& operator=(const Extrema_Circular_Buffer& other) {
        if(this != &other) {
            delete[] data;

            capacity = other.capacity;
            data = new Elem[CAPACITY_ACTUAL];
            idx_head = other.idx_head;
            idx_next_empty = other.idx_next_empty;

            for(size_t i = 0; i < CAPACITY_ACTUAL; ++i) {
                data[i] = other.data[i];
            }
        }
        return *this;
    }

    Extrema_Circular_Buffer& operator=(Extrema_Circular_Buffer&& other) {
        if(this != &other) {
            delete[] data;

            data = other.data;
            capacity = other.capacity;
            idx_head = other.idx_head;
            idx_next_empty = other.idx_next_empty;

            other.data = nullptr; // prevent deletion in destructor
        }
        return *this;
    }

    ~Extrema_Circular_Buffer() {
        delete[] data;
    }

    std::optional<T> pop_and_get_front_if_possible() {
        std::unique_lock<std::mutex> g0(mut_head);
        std::unique_lock<std::mutex> g1(mut_tail);

        if(empty_nonlocking())
            return std::optional<T>{};
        else if(2 <= size_nonlocking()) /* only allow simultaneous steal if there are at least 2 elements */
            g1.unlock();

        T& front = data[idx_head].first;

        std::optional<T> ret(std::move(front));

        idx_head = idx_next(idx_head);

        return ret;
    }

    /**
     * @brief if possible, steals and returns the tail element; else returns nothing
     * @return if 2 <= size_nonlocking(), then returns std::optional<T>{tail}; else returns std::optional<T>{};
     */
    std::optional<T> steal_if_possible() {
        std::unique_lock<std::mutex> g0{mut_head};
        std::unique_lock<std::mutex> g1{mut_tail};

        if(size_nonlocking() < 2)
            return std::optional<T>{};
        /* else */

        g0.unlock();

        auto idx_back = idx_prev(idx_next_empty);

        T& back = data[idx_back].first;
        bool back_stickied = data[idx_back].second;

        if(back_stickied)
            return std::optional<T>{};

        std::optional<T> ret(std::move(back));

        idx_next_empty = idx_back;

        return ret;
    }

    bool insert_tail_if_possible(T&& elem, bool stickied = false) {
        std::unique_lock<std::mutex> g0(mut_head);
        std::unique_lock<std::mutex> g1(mut_tail);

        if(full_nonlocking())
            return false;
        /* else */

        g0.unlock();

        data[idx_next_empty] = {std::move(elem), stickied};
        idx_next_empty = idx_next(idx_next_empty);

        return true;
    }

    bool insert_tail_if_possible(const T& elem, bool stickied = false) {
        return insert_tail_if_possible(T(elem), stickied);
    }

    bool empty() const {
        std::shared_lock<std::mutex> g0(mut_head);
        return empty_nonlocking();
    }

    bool full() const {
        std::shared_lock<std::mutex> g0(mut_tail);
        return full_nonlocking();
    }

    size_t size() const {
        std::shared_lock<std::mutex> g0(mut_head);
        return size_nonlocking();
    }

}; /* class Extrema_Circular_Buffer<T> */

template <typename T>
using Circular_Buffer = Extrema_Circular_Buffer<T>;

#endif /* CIRCULAR_BUFFER_H */
