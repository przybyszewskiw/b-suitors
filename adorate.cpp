#include "blimit.hpp"

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <queue>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <atomic>
#include <thread>
//#include <random>

/** Struktura do użycia w funkcji sort() sortującej sąsiadów wierzchołka. */
struct {
    bool operator()(std::pair<int, int> a, std::pair<int, int> b) const {
        if (a.second == b.second) return a.first > b.first;
        else return a.second > b.second;
    }
} pair_sort;

/** Klasa do użycia w kolejkach priorytetowych S odpowiednio sortująca
    wierzchołki, które oświadczyły się danemu. */
class less_pair_sort {
public:
    bool operator()(std::pair<int, int> a, std::pair<int, int> b) const {
        if (a.second == b.second) return a.first > b.first;
        else return a.second > b.second;
    }
};

/** Deklaracje zmiennych globalnych do użycia w całym programie. */
/** Liczba wierzchołków w całym grafie. */
std::atomic<int> vertices_number;
/** Numer wierzchołka, którego listę sąsiadów aktualnie sortujemy. */
std::atomic<int> vertex_to_sort;

/** Oryginalny graf reprezentowany, jako mapa - każemu wierzchołkowi
    przyporządkowujemy wektor krawędzi z niego wychodzących w formacie
    <nr_sąsiada, waga_krawędzi>. */
std::map<int, std::vector<std::pair<int, int>>> original_graph;
/** Nowy graf, który powstaje po przenumerowaniu wierzchołków. Teraz zamiast
    mapy możemy używać już wektora, bo teraz numery wierzchołków są kolejnymi
    liczbami całkowitymi. */
std::vector<std::vector<std::pair<int, int>>> new_graph;
/** Wektor, który przyporządkowuje nowemu numerowi wierzchołka, jego numer w
    oryginalnym grafie. */
std::vector<int> old_vertices;

/** Struktury wykorzystywane w algorytmie z oznaczeniami jak z niego. */
/** Typ kolejki priorytetowej przechowującej krawędzie wychodzące z jednego
    wierzchołka w formacie <nr_sąsiada, waga_krawędzi> i sortująca je
    według ustalonej już kolejności. */
using edges_queue_type = std::priority_queue<std::pair<int, int>,
                            std::vector<std::pair<int, int>>,
                            less_pair_sort>;
/** Deklaracje struktur podanych w algorytmie i (tablic) atomiców
    chroniących je.*/
std::vector<int> Q;
std::atomic<int> index_in_Q;
std::atomic<int> size_of_Q;
std::vector<int> R;
std::atomic<int> index_in_R;
edges_queue_type *S;
std::atomic<bool> *S_locks;

/** Tablica atomiców oznaczona w algorymie jako db, służący do aktualizacji
    wektora bvalues. */
std::atomic<int> *db;
/** Wektor z początkowymi wartościami funkcji b. */
std::vector<int> initial_bvalues;
/** Wektor z przyjmowanymi w danej iteracji wartościami b. */
std::vector<int> bvalues;
/** Wektor, w którym dla każdego wierchołka trzymamy informację, ilu spośród
    jego sąsiadów udało się nam przetworzyć. */
std::vector<int> last_neighbourgh;
/** Tablica z przenumerowanymi wierzchołkami posortowanymi od tego, z którego
    wychodzi najcięższa krawędź, do tego, z którego wychodzi najlżejsza. */
std::vector<int> sorted_vertices;

/** Tablica, w której trzymamy wagę najgorszego adoratora danego wierzchołka.
    Niekoniecznie musi być dobrze zaktualzowana, ale z pewnością jest nie lepsza
    od rzeczywistej wagi. */
std::atomic<int> *last_weight;
std::atomic<int> *last_adorator;

/** Funkcja wczytująca graf z danego pliku. */
void read_graph(std::string &input_filename) {
    std::ifstream input;
    input.open(input_filename);
    char c = input.peek();
    std::string comment_line;
    while (c == '#') {
        getline(input, comment_line);
        c = input.peek();
    }

    int a, b, w;
    while (input >> a >> b >>w) {
        original_graph[a].push_back(std::make_pair(b, w));
        original_graph[b].push_back(std::make_pair(a, w));
    }
}

/** Funkcja sortująca sąsiadów danego wierzchołka. */
void edge_sort (typename std::vector<std::pair<int, int>>::iterator beg,
            typename std::vector<std::pair<int, int>>::iterator end) {
    std::sort(beg, end, pair_sort);
}

/** Funkcja, którą wykonują wątki sortujące. */
void sort_neighbourghs() {
    while (true) {
        int my_vertex = vertex_to_sort++;
        if (my_vertex >= vertices_number) return;
        edge_sort(new_graph[my_vertex].begin(), new_graph[my_vertex].end());
    }
}

/** Funkcja równolegle sortująca listy sąsiadów wierzchołków. */
void parallel_sort_neighbourghs(int thread_count) {
    vertex_to_sort = 0;
    vertices_number = new_graph.size();
    std::vector<std::thread> sorting_threads;

    for (int i = 1; i < thread_count && i < vertices_number; i++) {
        sorting_threads.emplace_back(sort_neighbourghs);
    }

    sort_neighbourghs();

    for (int i = 1; i < thread_count && i < vertices_number; i++) {
        sorting_threads[i - 1].join();
    }
}

/** Funkcja tworząca nowy graf poprzez przenumerownaie wierzchołków z
    zachowaniem dotychczasowego porządku <. Dodatkowo tworzymy wektor
    old_vertices. */
void create_new_graph() {
    /** W pierwszej fazie każdemu wierzchołkowi przyporząkowujemy nowy numer. */
    std::unordered_map<int, int> new_vertices;
    int new_vertex_nr = 0;
    for (auto it = original_graph.begin(); it != original_graph.end(); ++it) {
        new_vertices[(*it).first] = new_vertex_nr;
        old_vertices.push_back((*it).first);
        ++new_vertex_nr;
    }

    /** W drugiej fazie faktycznie tworzymy opisane struktury. */
    for (auto it = original_graph.begin(); it != original_graph.end(); ++it) {
        std::vector<std::pair<int, int>> new_neighbourgs;
        for (int i = 0; i < (int)(*it).second.size(); i++) {
            int neighbourgh = (*it).second[i].first;
            int w = (*it).second[i].second;
            new_neighbourgs.push_back(std::make_pair(new_vertices[neighbourgh], w));
        }
        new_graph.push_back(new_neighbourgs);
    }
}

/** Funkcja sortująca wierzchołki względem najcięższej krawędzi z nich
    wychodzących. */
void sort_vertices() {
    std::vector<std::pair<int, int>> vertex_and_weight;
    for (int i = 0; i < vertices_number; i++) {
        vertex_and_weight.push_back(std::make_pair(i, new_graph[i][0].second));
    }
    std::sort(vertex_and_weight.begin(), vertex_and_weight.end(), pair_sort);
    for (int i = 0; i < vertices_number; i++) {
        sorted_vertices.push_back(vertex_and_weight[i].first);
    }
}

/** Główna funkcja, którą wykonuje pojedynczy wątek przetwarzający wierzchołek.
	Z grubsza wygląda tak samo, jak ta przedstawiona w pracy Khana i innych.
	Z uwagi na czytelność zdecydowałem nie dzielić się jej na mniejsze
	składowe, tylko skomentować kolejne czynności. */
void perform_algorithm(){
    while (true) {
		// Wybieramy swój wierzchołek, który będziemy przetwarzać
        int aux = index_in_Q++;
        if (aux >= size_of_Q) return;
        int u = Q[aux];

		// Przygotowujemy zmienną i potrzebną w danym przebiegu
        int i = 0;

        // Pętla, w której przeglądamy kolejnych sąsiadów u i sprawdzamy, czy są
        // odpowiedni dla u
        while (last_neighbourgh[u] < (int)new_graph[u].size() && i < bvalues[u]) {
            int p = new_graph[u][last_neighbourgh[u]].first;
            int w = new_graph[u][last_neighbourgh[u]].second;

			// Jeśli p nie możemy z nikim połączyć, to nie warto się nad nim
			// zastanawiać - zgodnie z FAQ niepotrzebne, ale na wszelki wypadek
            // zostawiam
            if (initial_bvalues[p] == 0) {
                last_neighbourgh[u]++;
                continue;
            }

            // Pierwsze sprawdzenie, dzięki tablicom last_weight i
            // last_adorator
            int p_last_size = last_weight[p];
            int p_last_adorator = last_adorator[p];
            if (p_last_size > w || (p_last_size == w && p_last_adorator > u)) {
                last_neighbourgh[u]++;
                continue;
            }

			// Teraz trzeba naprawdę sprawdzić, czy p jest dobrym parnterem,
            // tym razem lokując kolejkę S[p]
            p_last_size = -1;
            p_last_adorator = -1;

            bool almost_always_false = false;
            while (!S_locks[p].compare_exchange_weak(almost_always_false, true)) {
                almost_always_false = false;
            }

            if ((int)S[p].size() >= initial_bvalues[p]) {
                p_last_adorator = S[p].top().first;
                p_last_size = S[p].top().second;
            }
            if (p_last_size > w || (p_last_size == w && p_last_adorator > u)) {
                S_locks[p] = false;
                last_neighbourgh[u]++;
                continue;
            }

            // Jeśli dotarliśmy tutaj, to p jest odpowiednim parnterem dla u
            ++i;
            S[p].push(std::make_pair(u, w));

			// Ewentualnie usuwamy tego adoratora p, którego wyparł u
            if ((int)S[p].size() > initial_bvalues[p]) {
                int y = S[p].top().first;
                S[p].pop();
                aux = db[y]++;
                // W ten sposób każdy wierzchołek zostanie do R dodany
                // maksymalnie jeden raz
                if (aux == 0) {
                    aux = index_in_R++;
                    R[aux] = y;
                }
            }
            //Ewentualnie aktualizujemy last_weight
            if ((int)S[p].size() == initial_bvalues[p]) {
                last_adorator[p] = S[p].top().first;
                last_weight[p] = S[p].top().second;
            }
            S_locks[p] = false;
            last_neighbourgh[u]++;
        }
    }
}

/** Funkcja, która inicjalizuje struktury potrzebne do obliczania b-skojarzenia
    dla danej b-metody. */
void initialize_b_method(int b_method) {
    for (int i = 0; i < vertices_number; i++) {
        Q[i] = sorted_vertices[i];
        bvalues[i] = bvalue(b_method, old_vertices[i]);
        initial_bvalues[i] = bvalue(b_method, old_vertices[i]);
        db[i] = 0;
        last_neighbourgh[i] = 0;
        last_weight[i] = 0;
        last_adorator[i] = 0;
        S_locks[i] = false;
    }
    index_in_Q = 0;
    index_in_R = 0;
    size_of_Q = vertices_number.load();
    //std::random_shuffle(Q.begin(), Q.begin() + size_of_Q);
}

/** Funkcja znajdująca b-skojarzenie według algorytmu. */
void find_matching(int thread_count) {
    while (size_of_Q != 0) {
        /** Faza pierwsza - odpalamy kolejne wątki. */
        std::vector<std::thread> working_threads;

        for (int i = 1; i < thread_count && i < size_of_Q; i++) {
            working_threads.emplace_back(perform_algorithm);
        }

        perform_algorithm();

        for (int i = 1; i < thread_count && i < size_of_Q; i++) {
            working_threads[i - 1].join();
        }

        /** Faza druga - sprzątamy po danej iteracji i aktualizujemy kolejki,
            tablice oraz wektory. */
        std::swap(Q, R);
        size_of_Q = index_in_R.load();
        index_in_Q = 0;
        index_in_R = 0;
        for (int i = 0; i < vertices_number; i++) {
            bvalues[i] = db[i];
            db[i] = 0;
        }
    }
}

/** Funkcja, która oblicza sumę wag wybranych krawędzi, czyli wynik, dla danej
    metody, a także czyści kolejki S. */
int count_result() {
    int result = 0;
    for (int i = 0; i < vertices_number; ++i) {
        while (!S[i].empty()) {
            result += S[i].top().second;
            S[i].pop();
        }
    }
    return result / 2; // Bo każda krawędź jest policzona dwa razy
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "usage: "<<argv[0]<<" thread-count inputfile b-limit"<< std::endl;
        return 1;
    }

    int thread_count = std::stoi(argv[1]);
    int b_limit = std::stoi(argv[3]);
    std::string input_filename{argv[2]};

    read_graph(input_filename);

    create_new_graph();

    parallel_sort_neighbourghs(thread_count);

    sort_vertices();

    /** Inicjalizacja tablicowych i wektorowych struktur z algorytmu. */
    S = new edges_queue_type[vertices_number];
    S_locks = new std::atomic<bool>[vertices_number];
    db = new std::atomic<int>[vertices_number];
    last_weight = new std::atomic<int>[vertices_number];
    last_adorator = new std::atomic<int>[vertices_number];
    Q.resize(vertices_number);
    R.resize(vertices_number);
    bvalues.resize(vertices_number);
    initial_bvalues.resize(vertices_number);
    last_neighbourgh.resize(vertices_number);


    /** Przetwarzamy kolejne metody i wypisujemy dla nich wyniki. */
    for (int b_method = 0; b_method < b_limit + 1; b_method++) {
        initialize_b_method(b_method);

        find_matching(thread_count);

        int result = count_result();
        std::cout << result << "\n";
    }

    /** Zwolnienie zadeklarowanej pamięci. */
    delete[] S;
    delete[] S_locks;
    delete[] db;
    delete[] last_weight;
    delete[] last_adorator;
}
