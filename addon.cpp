#include "addon.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <sstream>
#include <unordered_set>

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

namespace addon {

namespace {

struct Token {
	enum class Type { Number, Op, Func, LParen, RParen, Comma } type{Type::Number};
	std::string text;
	double value{0.0};
	int precedence{0};
	bool rightAssoc{false};
	int arity{0};
};

static bool isIdentChar(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

static int funcArity(const std::string &name) {
	if (name == "min" || name == "max" || name == "pow") return 2;
	return 1;
}

static bool isFuncName(const std::string &name) {
	static const std::unordered_set<std::string> funcs = {
		"sin", "cos", "tan", "tanh", "exp", "log", "sqrt", "abs", "min", "max", "pow"
	};
	return funcs.count(name) > 0;
}

static std::vector<Token> tokenizeExpr(const std::string &expr, bool &ok) {
	ok = true;
	std::vector<Token> out;
	Token prev;
	bool hasPrev = false;
	for (size_t i = 0; i < expr.size();) {
		char c = expr[i];
		if (std::isspace(static_cast<unsigned char>(c))) { i++; continue; }
		if (std::isdigit(static_cast<unsigned char>(c)) || c == '.') {
			const char *s = expr.c_str() + i;
			char *end = nullptr;
			double v = std::strtod(s, &end);
			if (end == s) { ok = false; return {}; }
			Token t; t.type = Token::Type::Number; t.value = v; t.text = std::string(s, (size_t)(end - s));
			out.push_back(t);
			i = static_cast<size_t>(end - expr.c_str());
			hasPrev = true; prev = t;
			continue;
		}
		if (isIdentChar(c)) {
			size_t start = i;
			while (i < expr.size() && isIdentChar(expr[i])) i++;
			std::string name = expr.substr(start, i - start);
			Token t;
			if (isFuncName(name)) {
				t.type = Token::Type::Func;
				t.text = name;
				t.arity = funcArity(name);
			} else if (name == "pi" || name == "PI") {
				t.type = Token::Type::Number;
				t.value = 3.14159265358979323846;
			} else if (name == "e" || name == "E") {
				t.type = Token::Type::Number;
				t.value = 2.71828182845904523536;
			} else {
				ok = false;
				return {};
			}
			out.push_back(t);
			hasPrev = true; prev = t;
			continue;
		}
		if (c == '(') {
			Token t; t.type = Token::Type::LParen; t.text = "("; out.push_back(t);
			hasPrev = true; prev = t; i++; continue;
		}
		if (c == ')') {
			Token t; t.type = Token::Type::RParen; t.text = ")"; out.push_back(t);
			hasPrev = true; prev = t; i++; continue;
		}
		if (c == ',') {
			Token t; t.type = Token::Type::Comma; t.text = ","; out.push_back(t);
			hasPrev = true; prev = t; i++; continue;
		}
		if (c == '+' || c == '-' || c == '*' || c == '/' || c == '^') {
			Token t; t.type = Token::Type::Op; t.text = std::string(1, c);
			bool unary = false;
			if (!hasPrev || prev.type == Token::Type::Op || prev.type == Token::Type::LParen || prev.type == Token::Type::Comma) {
				unary = (c == '-');
			}
			if (unary) {
				t.text = "~";
				t.precedence = 4;
				t.rightAssoc = true;
				t.arity = 1;
			} else if (c == '+' || c == '-') {
				t.precedence = 1;
				t.rightAssoc = false;
				t.arity = 2;
			} else if (c == '*' || c == '/') {
				t.precedence = 2;
				t.rightAssoc = false;
				t.arity = 2;
			} else if (c == '^') {
				t.precedence = 3;
				t.rightAssoc = true;
				t.arity = 2;
			}
			out.push_back(t);
			hasPrev = true; prev = t; i++; continue;
		}
		ok = false;
		return {};
	}
	return out;
}

static std::vector<Token> toRpn(const std::vector<Token> &tokens, bool &ok) {
	ok = true;
	std::vector<Token> output;
	std::vector<Token> ops;
	for (const auto &tok : tokens) {
		if (tok.type == Token::Type::Number) {
			output.push_back(tok);
			continue;
		}
		if (tok.type == Token::Type::Func) {
			ops.push_back(tok);
			continue;
		}
		if (tok.type == Token::Type::Comma) {
			while (!ops.empty() && ops.back().type != Token::Type::LParen) {
				output.push_back(ops.back());
				ops.pop_back();
			}
			if (ops.empty()) { ok = false; return {}; }
			continue;
		}
		if (tok.type == Token::Type::Op) {
			while (!ops.empty()) {
				const auto &top = ops.back();
				if (top.type != Token::Type::Op) break;
				if ((tok.rightAssoc && tok.precedence < top.precedence) || (!tok.rightAssoc && tok.precedence <= top.precedence)) {
					output.push_back(top);
					ops.pop_back();
				} else {
					break;
				}
			}
			ops.push_back(tok);
			continue;
		}
		if (tok.type == Token::Type::LParen) { ops.push_back(tok); continue; }
		if (tok.type == Token::Type::RParen) {
			while (!ops.empty() && ops.back().type != Token::Type::LParen) {
				output.push_back(ops.back());
				ops.pop_back();
			}
			if (ops.empty()) { ok = false; return {}; }
			ops.pop_back();
			if (!ops.empty() && ops.back().type == Token::Type::Func) {
				output.push_back(ops.back());
				ops.pop_back();
			}
			continue;
		}
		ok = false;
		return {};
	}
	while (!ops.empty()) {
		if (ops.back().type == Token::Type::LParen || ops.back().type == Token::Type::RParen) {
			ok = false;
			return {};
		}
		output.push_back(ops.back());
		ops.pop_back();
	}
	return output;
}

static double evalRpn(const std::vector<Token> &rpn, bool &ok) {
	ok = true;
	std::vector<double> st;
	for (const auto &tok : rpn) {
		if (tok.type == Token::Type::Number) {
			st.push_back(tok.value);
			continue;
		}
		if (tok.type == Token::Type::Op) {
			if (tok.arity == 1) {
				if (st.empty()) { ok = false; return 0.0; }
				double a = st.back(); st.pop_back();
				st.push_back(-a);
				continue;
			}
			if (st.size() < 2) { ok = false; return 0.0; }
			double b = st.back(); st.pop_back();
			double a = st.back(); st.pop_back();
			double v = 0.0;
			if (tok.text == "+") v = a + b;
			else if (tok.text == "-") v = a - b;
			else if (tok.text == "*") v = a * b;
			else if (tok.text == "/") v = b == 0.0 ? std::numeric_limits<double>::quiet_NaN() : a / b;
			else if (tok.text == "^") v = std::pow(a, b);
			else { ok = false; return 0.0; }
			st.push_back(v);
			continue;
		}
		if (tok.type == Token::Type::Func) {
			if (tok.arity == 1) {
				if (st.empty()) { ok = false; return 0.0; }
				double a = st.back(); st.pop_back();
				double v = 0.0;
				if (tok.text == "sin") v = std::sin(a);
				else if (tok.text == "cos") v = std::cos(a);
				else if (tok.text == "tan") v = std::tan(a);
				else if (tok.text == "tanh") v = std::tanh(a);
				else if (tok.text == "exp") v = std::exp(a);
				else if (tok.text == "log") v = std::log(a);
				else if (tok.text == "sqrt") v = a < 0.0 ? std::numeric_limits<double>::quiet_NaN() : std::sqrt(a);
				else if (tok.text == "abs") v = std::fabs(a);
				else { ok = false; return 0.0; }
				st.push_back(v);
				continue;
			}
			if (st.size() < 2) { ok = false; return 0.0; }
			double b = st.back(); st.pop_back();
			double a = st.back(); st.pop_back();
			double v = 0.0;
			if (tok.text == "min") v = std::min(a, b);
			else if (tok.text == "max") v = std::max(a, b);
			else if (tok.text == "pow") v = std::pow(a, b);
			else { ok = false; return 0.0; }
			st.push_back(v);
			continue;
		}
		ok = false;
		return 0.0;
	}
	if (st.size() != 1 || !std::isfinite(st.back())) { ok = false; return 0.0; }
	return st.back();
}

static bool looksLikeExpr(const std::string &text) {
	std::string t = text;
	auto trim = [&](std::string s) {
		auto start = s.find_first_not_of(" \t\r\n");
		if (start == std::string::npos) return std::string();
		auto end = s.find_last_not_of(" \t\r\n");
		return s.substr(start, end - start + 1);
	};
	t = trim(t);
	if (t.empty()) return false;
	bool hasOp = false;
	for (char c : t) {
		if (std::isdigit(static_cast<unsigned char>(c))) continue;
		if (std::isspace(static_cast<unsigned char>(c))) continue;
		if (c == '+' || c == '-' || c == '*' || c == '/' || c == '^' || c == '(' || c == ')' || c == '.' || c == ',' ) { hasOp = true; continue; }
		if (isIdentChar(c)) { hasOp = true; continue; }
		return false;
	}
	return hasOp;
}

static std::string extractExpr(const std::string &text) {
	std::string t = text;
	std::string lower = t;
	std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c){ return (char)std::tolower(c); });
	std::vector<std::string> prefixes = {"calc:", "math:", "计算:", "求值:", "calc=", "math=", "计算=", "求值="};
	for (const auto &p : prefixes) {
		if (lower.rfind(p, 0) == 0) return t.substr(p.size());
	}
	return t;
}

class MathAddon : public Addon {
public:
	explicit MathAddon(std::string name) : name_(std::move(name)) {}
	std::string name() const override { return name_; }
	std::string type() const override { return "math"; }
	AddonResult handle(const std::string &text, const json &payload) override {
		(void)payload;
		AddonResult res;
		std::string expr = extractExpr(text);
		if (!looksLikeExpr(expr)) return res;
		bool ok = true;
		auto tokens = tokenizeExpr(expr, ok);
		if (!ok) return res;
		auto rpn = toRpn(tokens, ok);
		if (!ok) return res;
		double v = evalRpn(rpn, ok);
		if (!ok || !std::isfinite(v)) return res;
		std::ostringstream oss;
		oss.setf(std::ios::fixed);
		oss.precision(10);
		oss << v;
		std::string value = oss.str();
		value.erase(value.find_last_not_of('0') + 1);
		if (!value.empty() && value.back() == '.') value.pop_back();
		res.handled = true;
		res.reply = "结果: " + value;
		res.meta = json{{"addon", "math"}, {"name", name_}, {"expression", expr}, {"value", v}};
		return res;
	}

private:
	std::string name_;
};

} // namespace

void AddonManager::registerAddon(std::shared_ptr<Addon> addon) {
	if (!addon) return;
	std::lock_guard<std::mutex> lock(mu_);
	addRecord(addon, "builtin", "", nullptr, nullptr);
}

bool AddonManager::addBuiltin(const std::string &type, const std::string &name, std::string *error) {
	std::string key = name.empty() ? type : name;
	if (key.empty()) {
		if (error) *error = "name required";
		return false;
	}
	std::shared_ptr<Addon> addon;
	if (type == "math") {
		addon = std::make_shared<MathAddon>(key);
	} else {
		if (error) *error = "unsupported addon type";
		return false;
	}
	std::lock_guard<std::mutex> lock(mu_);
	return addRecord(addon, "builtin", "", nullptr, error);
}

bool AddonManager::loadLibrary(const std::string &path, std::string *error) {
	if (path.empty()) {
		if (error) *error = "path required";
		return false;
	}
#ifdef _WIN32
	HMODULE lib = LoadLibraryA(path.c_str());
	if (!lib) {
		if (error) *error = "failed to load library";
		return false;
	}
	auto apiFn = (int (*)())GetProcAddress(lib, "addon_api_version");
	auto createFn = (Addon *(*)())GetProcAddress(lib, "addon_create_v1");
	auto destroyFn = (void (*)(Addon *))GetProcAddress(lib, "addon_destroy_v1");
#else
	void *lib = dlopen(path.c_str(), RTLD_NOW);
	if (!lib) {
		if (error) *error = dlerror();
		return false;
	}
	auto apiFn = (int (*)())dlsym(lib, "addon_api_version");
	auto createFn = (Addon *(*)())dlsym(lib, "addon_create_v1");
	auto destroyFn = (void (*)(Addon *))dlsym(lib, "addon_destroy_v1");
#endif
	if (!createFn || !destroyFn) {
		if (error) *error = "missing addon_create_v1/addon_destroy_v1";
#ifdef _WIN32
		FreeLibrary(lib);
#else
		dlclose(lib);
#endif
		return false;
	}
	if (apiFn && apiFn() != 1) {
		if (error) *error = "unsupported addon api version";
#ifdef _WIN32
		FreeLibrary(lib);
#else
		dlclose(lib);
#endif
		return false;
	}
	Addon *raw = createFn();
	if (!raw) {
		if (error) *error = "addon_create_v1 returned null";
#ifdef _WIN32
		FreeLibrary(lib);
#else
		dlclose(lib);
#endif
		return false;
	}
	std::shared_ptr<Addon> addon(raw, [destroyFn](Addon *p) { destroyFn(p); });
	std::lock_guard<std::mutex> lock(mu_);
	if (!addRecord(addon, "library", path, lib, error)) {
		addon.reset();
#ifdef _WIN32
		FreeLibrary(lib);
#else
		dlclose(lib);
#endif
		return false;
	}
	return true;
}

bool AddonManager::removeAddon(const std::string &name, std::string *error) {
	if (name.empty()) {
		if (error) *error = "name required";
		return false;
	}
	std::lock_guard<std::mutex> lock(mu_);
	auto it = addonIndex_.find(name);
	if (it == addonIndex_.end()) {
		if (error) *error = "addon not found";
		return false;
	}
	size_t idx = it->second;
	if (idx < addons_.size()) {
		unloadRecord(addons_[idx]);
		addons_.erase(addons_.begin() + (std::ptrdiff_t)idx);
	}
	addonIndex_.clear();
	for (size_t i = 0; i < addons_.size(); i++) addonIndex_[addons_[i].name] = i;
	return true;
}

json AddonManager::listAddons() const {
	std::lock_guard<std::mutex> lock(mu_);
	json out = json::array();
	for (const auto &rec : addons_) {
		out.push_back(json{{"name", rec.name}, {"type", rec.type}, {"source", rec.source}, {"path", rec.path}});
	}
	return out;
}

AddonResult AddonManager::run(const std::string &text, const json &payload) const {
	AddonResult combined;
	std::lock_guard<std::mutex> lock(mu_);
	for (const auto &rec : addons_) {
		if (!rec.addon) continue;
		auto res = rec.addon->handle(text, payload);
		if (res.handled) return res;
		if (!res.extraTokens.empty()) {
			combined.extraTokens.insert(combined.extraTokens.end(), res.extraTokens.begin(), res.extraTokens.end());
		}
	}
	return combined;
}

std::shared_ptr<AddonManager> createDefaultAddons() {
	auto mgr = std::make_shared<AddonManager>();
	mgr->registerAddon(std::make_shared<MathAddon>("math"));
	return mgr;
}

bool AddonManager::addRecord(const std::shared_ptr<Addon> &addon,
						 const std::string &source,
						 const std::string &path,
						 void *libHandle,
						 std::string *error) {
	if (!addon) {
		if (error) *error = "addon is null";
		return false;
	}
	std::string key = addon->name();
	if (key.empty()) {
		if (error) *error = "addon name required";
		return false;
	}
	if (addonIndex_.count(key)) {
		if (error) *error = "addon already exists";
		return false;
	}
	AddonRecord rec;
	rec.addon = addon;
	rec.name = key;
	rec.type = addon->type();
	rec.source = source;
	rec.path = path;
	rec.libHandle = libHandle;
	addonIndex_[key] = addons_.size();
	addons_.push_back(std::move(rec));
	return true;
}

void AddonManager::unloadRecord(AddonRecord &rec) {
	rec.addon.reset();
	if (rec.libHandle) {
#ifdef _WIN32
		FreeLibrary((HMODULE)rec.libHandle);
#else
		dlclose(rec.libHandle);
#endif
		rec.libHandle = nullptr;
	}
}

} // namespace addon
