#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <random>
#include <chrono>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace transformer {

using json = nlohmann::json;

struct TrainSample {
	std::string input;
	std::string target;
	std::string graph;
};

struct TransformerParams {
	int vocabSize{8192};
	int dModel{128};
	int nHeads{4};
	int nLayers{2};
	int dFF{256};
	int maxLen{512};
	int maxTokens{256};
	float lr{0.001f};
	bool useLrSchedule{true};
	int lrWarmupSteps{200};
	int lrCosineSteps{2000};
	float lrMinRatio{0.1f};
	int gradAccumSteps{1};
	float lossNoiseAlpha{0.0f};
	int attnChunkSize{0};
	float graphWeight{0.35f};
	float verifyThreshold{0.28f};
	float temperature{1.0f};
	bool dynamicSampling{false};
	float temperatureMin{0.0f};
	float temperatureMax{0.0f};
	int topK{0};
	float topP{0.0f};
	float topPMin{0.0f};
	float topPMax{0.0f};
	int dynamicWindow{64};
	float dynamicRepetitionBoost{0.25f};
	float labelSmoothing{0.0f};
	float gradClip{1.0f};
	float adamBeta1{0.9f};
	float adamBeta2{0.999f};
	float adamEps{1e-8f};
	float weightDecay{0.0f};
	float repetitionPenalty{1.0f};
	bool repetitionSegmented{false};
	int repetitionWindowRecent{32};
	int repetitionWindowMid{128};
	float repetitionPenaltyRecent{1.2f};
	float repetitionPenaltyMid{1.1f};
	float repetitionPenaltyOld{1.05f};
	int noRepeatNgram{0};
	int minNewTokens{0};
	bool enableDiagnostics{false};
	float bidirectionalWeight{0.0f};
	float selfFlowWeight{0.0f};
	int cacheMaxEntries{256};
	int cacheTokenEntries{512};
	int cacheTtlMs{120000};
	std::string tokenizerMode{"hash"};
};

struct Matrix {
	int rows{0};
	int cols{0};
	std::vector<float> data;

	Matrix() = default;
	Matrix(int r, int c, float v = 0.0f) : rows(r), cols(c), data((size_t)r * (size_t)c, v) {}

	float &operator()(int r, int c) { return data[(size_t)r * (size_t)cols + (size_t)c]; }
	float operator()(int r, int c) const { return data[(size_t)r * (size_t)cols + (size_t)c]; }
};

struct Linear {
	Matrix w;
	std::vector<float> b;
	std::vector<float> mW;
	std::vector<float> vW;
	std::vector<float> mB;
	std::vector<float> vB;
	std::vector<float> gW;
	std::vector<float> gB;
	int step{0};

	Linear() = default;
	Linear(int in, int out);
	std::vector<float> forward(const std::vector<float> &x) const;
};

struct LayerNorm {
	std::vector<float> gamma;
	std::vector<float> beta;
	std::vector<float> mGamma;
	std::vector<float> vGamma;
	std::vector<float> mBeta;
	std::vector<float> vBeta;
	std::vector<float> gGamma;
	std::vector<float> gBeta;
	int step{0};
	float eps{1e-5f};

	LayerNorm() = default;
	explicit LayerNorm(int d);
	std::vector<float> forward(const std::vector<float> &x) const;
};

struct MultiHeadAttention {
	int nHeads{4};
	int dModel{128};
	int dHead{32};
	Linear wq;
	Linear wk;
	Linear wv;
	Linear wo;

	MultiHeadAttention() = default;
	MultiHeadAttention(int dModel, int nHeads);
	std::vector<std::vector<float>> forward(const std::vector<std::vector<float>> &x,
											bool causal,
											const std::vector<std::vector<float>> *memory) const;
};

struct FeedForward {
	Linear w1;
	Linear w2;

	FeedForward() = default;
	FeedForward(int dModel, int dFF);
	std::vector<float> forward(const std::vector<float> &x) const;
};

struct EncoderLayer {
	MultiHeadAttention selfAttn;
	FeedForward ffn;
	LayerNorm ln1;
	LayerNorm ln2;

	EncoderLayer() = default;
	EncoderLayer(int dModel, int nHeads, int dFF);
	std::vector<std::vector<float>> forward(const std::vector<std::vector<float>> &x) const;
};

struct DecoderLayer {
	MultiHeadAttention selfAttn;
	MultiHeadAttention crossAttn;
	FeedForward ffn;
	LayerNorm ln1;
	LayerNorm ln2;
	LayerNorm ln3;

	DecoderLayer() = default;
	DecoderLayer(int dModel, int nHeads, int dFF);
	std::vector<std::vector<float>> forward(const std::vector<std::vector<float>> &x,
											const std::vector<std::vector<float>> &memory) const;
};

class Tokenizer {
public:
	explicit Tokenizer(int vocabSize, const std::string &mode = "hash");
	std::vector<int> encode(const std::string &text, int maxLen, bool addBosEos) const;
	std::string decode(const std::vector<int> &ids) const;
	const std::string &mode() const { return mode_; }
	void setMode(const std::string &mode);

private:
	int vocabSize_{8192};
	std::string mode_{"hash"};
};

class TransformerModel {
public:
	explicit TransformerModel(const TransformerParams &params);
	const TransformerParams &params() const { return params_; }

	std::vector<std::vector<float>> encode(const std::vector<int> &tokens) const;
	std::vector<std::vector<float>> decode(const std::vector<int> &tokens,
										   const std::vector<std::vector<float>> &memory) const;
	std::vector<float> logitsAt(const std::vector<float> &hidden) const;
	std::vector<int> generate(const std::vector<int> &inputTokens,
						  const std::vector<int> &graphTokens,
					  int maxTokens,
					  float graphWeight) const;
	std::vector<int> generate(const std::vector<int> &inputTokens,
						  const std::vector<int> &graphTokens,
					  const std::vector<std::vector<float>> &graphEmbeddings,
					  int maxTokens,
					  float graphWeight) const;
	std::vector<std::vector<float>> fuseMemory(const std::vector<std::vector<float>> &textMem,
										 const std::vector<std::vector<float>> &graphMem,
										 float graphWeight) const;
	float trainOnSample(const std::vector<int> &inputTokens,
						const std::vector<int> &graphTokens,
						const std::vector<int> &targetTokens,
						float lr);

	json toJson() const;
	void updateParams(const TransformerParams &p);

private:
	TransformerParams params_;
	Matrix tokEmbed_;
	Matrix posEmbed_;
	Linear outProj_;
	MultiHeadAttention fuseAttn_;
	Linear fuseGate_;
	std::vector<EncoderLayer> encLayers_;
	std::vector<DecoderLayer> decLayers_;
	Tokenizer tokenizer_;
	std::vector<float> outMom1_;
	std::vector<float> outMom2_;
	std::vector<float> outGradW_;
	std::vector<float> outGradB_;
	int outStep_{0};
	std::vector<float> tokMom1_;
	std::vector<float> tokMom2_;
	std::vector<float> posMom1_;
	std::vector<float> posMom2_;
	std::vector<float> tokGrad_;
	std::vector<float> posGrad_;
	int embedStep_{0};
	int trainStep_{0};
	float lossEma_{0.0f};

	std::vector<std::vector<float>> embedTokens(const std::vector<int> &tokens) const;
};

class TransformerService {
public:
	TransformerService();

	json chat(const std::string &text, const std::string &graphContext, int maxTokens);
	json chat(const std::string &text, const std::string &graphContext,
			  const std::vector<std::vector<float>> &graphEmbeddings, int maxTokens);
	json pretrain(const std::vector<TrainSample> &samples, int epochs, float lr);
	json jointTrain(const std::vector<TrainSample> &samples, int epochs, float lr, float graphWeight);
	json optimizeGA(const std::vector<TrainSample> &samples, int generations, int population);
	json verify(const std::string &text, const std::string &graphContext, const std::string &reply) const;
	json params() const;
	void applyParams(const json &patch);

private:
	TransformerParams params_;
	TransformerModel model_;
	Tokenizer tokenizer_;
	std::mt19937 rng_{0xC0FFEEu};
	mutable std::mutex cacheMu_;
	std::list<std::string> replyOrder_;
	std::unordered_map<std::string, std::pair<json, std::pair<int64_t, std::list<std::string>::iterator>>> replyCache_;
	std::list<std::string> tokenOrder_;
	std::unordered_map<std::string, std::pair<std::vector<int>, std::list<std::string>::iterator>> tokenCache_;

	float sampleLoss(const TrainSample &s);
	float jaccard(const std::vector<int> &a, const std::vector<int> &b) const;
	float computeContrastiveLoss(const std::vector<int> &a, const std::vector<int> &b) const;
};

} // namespace transformer
