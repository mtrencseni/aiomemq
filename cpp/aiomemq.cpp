#include <boost/asio.hpp>
#include <boost/json.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <deque>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <unordered_map>

using namespace std;
namespace asio = boost::asio;
namespace js   = boost::json;
using asio::ip::tcp;

constexpr int DEFAULT_PORT       = 7000;
constexpr int DEFAULT_CACHE_SIZE = 100;
static    int cache_size         = DEFAULT_CACHE_SIZE; // set at runtime

// forwards
class Session;
using SessionPtr = shared_ptr<Session>;

// global state
unordered_map<string, set<SessionPtr>>   topics;     // topic   -> subscribers
unordered_map<SessionPtr, set<string>>   topics_rev; // session -> subscribed topics
unordered_map<string, deque<js::object>> caches;     // topic   -> ring buffer
unordered_map<string, int>               indexs;     // topic   -> next index

// random engine for "delivery: one"
random_device rd;
mt19937 rng { rd() };

// helpers
void trim_cache(deque<js::object>& q)
{
    while(static_cast<int>(q.size()) > cache_size)
        q.pop_front();
}

void send_raw(tcp::socket& s, const string& buf)
{
    asio::write(s, asio::buffer(buf));
}

void send_cmd(tcp::socket& s, const js::object& obj)
{
    send_raw(s, js::serialize(obj) + "\r\n");
}

void send_success(tcp::socket& s)
{
    send_cmd(s, {{"success", true}});
}

void send_failure(tcp::socket& s, const string& why)
{
    send_cmd(s, {{"success", false}, {"reason", why}});
}

bool valid_utf8(const string& str)
{
    try {
        boost::locale::conv::utf_to_utf<char32_t>(str.c_str(), str.c_str() + str.size());
        return true;
    }
    catch(const boost::locale::conv::conversion_error&) {
        return false;
    }
    // const unsigned char* u = reinterpret_cast<const unsigned char*>(str.data());
    // size_t i = 0, n = str.size();
    // while (i < n) {
    //     unsigned char c = u[i];
    //     if (c <= 0x7F) { ++i; continue; } // ASCII
    //     size_t cont = 0;
    //     if ((c & 0xE0) == 0xC0) { cont = 1; if (c < 0xC2) return false; }
    //     else if ((c & 0xF0) == 0xE0) cont = 2;
    //     else if ((c & 0xF8) == 0xF0) { cont = 3; if (c > 0xF4) return false; }
    //     else return false;
    //     if (i + cont >= n) return false;
    //     for (size_t j = 1; j <= cont; ++j)
    //         if ((u[i + j] & 0xC0) != 0x80) return false;
    //     i += cont + 1;
    // }
    // return true;
}

void send_cached(tcp::socket& s, const string& topic, int last_seen)
{
    auto& q = caches[topic];

    for (auto& m : q) // unseen messages
        if (m.at("index").as_int64() > last_seen)
            send_cmd(s, m);

    deque<js::object> new_q; // rebuild cache
    for (auto& m : q) {
        int idx = m.at("index").as_int64();
        if (idx <= last_seen || m.at("delivery") == "all")
            new_q.push_back(m);
    }
    q.swap(new_q);
    trim_cache(q);
}

// schema validation
struct FieldSpec {
    js::kind kind;
    bool required;
    vector<js::value> values; // allowed literals (optional)
};
using Template = unordered_map<string, FieldSpec>;

const Template template_subscribe {
    {"command",   {js::kind::string, true , {}}},
    {"topic",     {js::kind::string, true , {}}},
    {"last_seen", {js::kind::int64 , false, {}}},
    {"cache",     {js::kind::bool_ , false, {}}}
};

const Template template_unsubscribe {
    {"command",   {js::kind::string, true , {}}},
    {"topic",     {js::kind::string, true , {}}}
};

const Template template_send {
    {"command",   {js::kind::string, true , {}}},
    {"topic",     {js::kind::string, true , {}}},
    {"msg",       {js::kind::string, true , {}}},
    {"delivery",  {js::kind::string, true , {js::value("all"), js::value("one")}}},
    {"cache",     {js::kind::bool_ , false, {}}}
};

bool template_match(const Template& t, const js::object& o)
{
    for (auto& [k, _] : o)
        if (!t.contains(k))
            return false;

    for (auto& [k, spec] : t) {
        if (spec.required && !o.contains(k))
            return false;
        if (!o.contains(k))
            continue;

        const js::value& v = o.at(k);
        if (v.kind() != spec.kind)
            return false;

        if (spec.values.empty())
            continue;
        if (find(spec.values.begin(), spec.values.end(), v) == spec.values.end())
            return false;
    }
    return true;
}

bool verify_command(const js::object& o)
{
    auto it = o.find("command");
    if (it == o.end() || it->value().kind() != js::kind::string)
        return false;

    string cmd = string(it->value().as_string());
    if (cmd == "subscribe")
        return template_match(template_subscribe, o);
    if (cmd == "unsubscribe")
        return template_match(template_unsubscribe, o);
    if (cmd == "send")
        return template_match(template_send, o);

    return false;
}

// per-client session
class Session : public enable_shared_from_this<Session>
{
    tcp::socket     sock_;
    asio::streambuf sbuf_;

public:
    explicit Session(tcp::socket s) : sock_(std::move(s)) {}
    void start() { do_read(); }
    tcp::socket& socket() { return sock_; }

private:
    void do_read()
    {
        auto self = shared_from_this();
        asio::async_read_until(sock_, sbuf_, '\n', [self,this](boost::system::error_code sys_err, size_t /*len*/)
        {
            if (sys_err)
                return cleanup();

            string line;
            {
                istream is(&sbuf_);
                getline(is, line);
                if (!line.empty() && line.back() == '\r')
                    line.pop_back();
            }

            if (line == "quit")
                return cleanup();
            if (line.empty())
                return do_read();

            if (!valid_utf8(line)) {
                send_failure(sock_, "Could not decode input as UTF-8");
                return do_read();
            }

            js::error_code js_err;
            js::value val = js::parse(line, js_err);

            if (js_err) {
                send_failure(sock_, "Could not parse json");
                return do_read();
            }
            if (!val.is_object() || !verify_command(val.as_object())) {
                send_failure(sock_, "Malformed json message");
                return do_read();
            }

            handle_command(val.as_object());
            do_read();
        });
    }

    void handle_command(js::object cmd) {
        string c = string(cmd.at("command").as_string());
        if (c == "subscribe")
            handle_subscribe(cmd);
        else if (c == "unsubscribe")
            handle_unsubscribe(cmd);
        else
            handle_send(cmd);
    }

    void handle_subscribe(const js::object& cmd)
    {
        string topic = string(cmd.at("topic").as_string());
        SessionPtr self = shared_from_this();

        topics[topic].insert(self);
        topics_rev[self].insert(topic);

        int last_seen = -1;
        if (auto it = cmd.find("last_seen"); it != cmd.end())
            last_seen = static_cast<int>(it->value().as_int64());

        send_success(sock_);

        bool want_cache = cmd.contains("cache") ? cmd.at("cache").as_bool() : true;
        if (want_cache)
            send_cached(sock_, topic, last_seen);
    }

    void handle_unsubscribe(const js::object& cmd)
    {
        string topic = string(cmd.at("topic").as_string());
        SessionPtr self = shared_from_this();

        topics[topic].erase(self);
        topics_rev[self].erase(topic);
        send_success(sock_);
    }

    void handle_send(js::object& cmd)
    {
        string topic    = string(cmd.at("topic").as_string());
        string delivery = string(cmd.at("delivery").as_string());
        bool   do_cache = cmd.contains("cache") ? cmd.at("cache").as_bool() : true;

        int idx = indexs[topic]++;
        cmd["index"] = idx;

        set<SessionPtr> recipients;
        if (delivery == "all") {
            recipients = topics[topic];
        } else { // "one"
            auto& subs = topics[topic];
            if (!subs.empty()) {
                uniform_int_distribution<size_t> dist(0, subs.size() - 1);
                auto it = subs.begin();
                advance(it, dist(rng));
                recipients.insert(*it);
                do_cache = false;
            }
        }

        if (do_cache) {
            auto& q = caches[topic];
            q.push_back(cmd);
            trim_cache(q);
        }

        for (auto& s : recipients)
            send_cmd(s->socket(), cmd);
        send_success(sock_);
    }

    void cleanup()
    {
        SessionPtr self = shared_from_this();

        if (auto it = topics_rev.find(self); it != topics_rev.end()) {
            for (auto& t : it->second)
                topics[t].erase(self);
            topics_rev.erase(it);
        }
        boost::system::error_code ec;
        sock_.shutdown(tcp::socket::shutdown_both, ec);
        sock_.close(ec);
    }
};

class Server
{
    asio::io_context& io_;
    tcp::acceptor     acc_;

public:
    Server(asio::io_context& io, uint16_t port)
        : io_(io),
          acc_(io, tcp::endpoint(asio::ip::make_address("127.0.0.1"), port))
    {   do_accept(); }

private:
    void do_accept()
    {
        acc_.async_accept([this](boost::system::error_code err, tcp::socket s) {
            if (!err)
                make_shared<Session>(std::move(s))->start();
            do_accept();
        });
    }
};

int main(int argc, char* argv[])
{
    if (argc != 1 && argc != 2 && argc != 3) {
        cerr << "Usage: aiomemq <port> <cache_size>\n";
        cerr << "  <port>       - optional, default " << DEFAULT_PORT << '\n';
        cerr << "  <cache_size> - optional, default " << DEFAULT_CACHE_SIZE << '\n';
        return 1;
    }

    int port = DEFAULT_PORT;
    if (argc >= 2) port = stoi(argv[1]);
    if (argc == 3) cache_size = stoi(argv[2]);

    asio::io_context io;
    Server srv(io, static_cast<uint16_t>(port));

    cerr << "Listening on 127.0.0.1:" << port << "\n";
    io.run();
}

// build:
//   g++ -w -Iboost -Lboost/stage/lib -o aiomemq aiomemq.cpp -Wl,-Bstatic -lboost_json -Wl,-Bdynamic -std=c++20
