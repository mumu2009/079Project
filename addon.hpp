#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

namespace addon {

using json = nlohmann::json;

struct AddonResult {
	bool handled{false};
	std::string reply;
	std::vector<std::string> extraTokens;
	json meta = json::object();
};

class Addon {
public:
	virtual ~Addon() = default;
	virtual std::string name() const = 0;
	virtual std::string type() const = 0;
	virtual AddonResult handle(const std::string &text, const json &payload) = 0;
};

class AddonManager {
public:
	void registerAddon(std::shared_ptr<Addon> addon);
	bool addBuiltin(const std::string &type, const std::string &name, std::string *error = nullptr);
	bool loadLibrary(const std::string &path, std::string *error = nullptr);
	bool removeAddon(const std::string &name, std::string *error = nullptr);
	json listAddons() const;
	AddonResult run(const std::string &text, const json &payload) const;

private:
	struct AddonRecord {
		std::shared_ptr<Addon> addon;
		std::string name;
		std::string type;
		std::string source;
		std::string path;
		void *libHandle{nullptr};
	};

	bool addRecord(const std::shared_ptr<Addon> &addon,
				   const std::string &source,
				   const std::string &path,
				   void *libHandle,
				   std::string *error);
	void unloadRecord(AddonRecord &rec);

	mutable std::mutex mu_;
	std::vector<AddonRecord> addons_;
	std::unordered_map<std::string, size_t> addonIndex_;
};

std::shared_ptr<AddonManager> createDefaultAddons();

} // namespace addon
