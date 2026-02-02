#include "transformer.hpp"

#include <algorithm>
#include <cmath>
#include <random>
#include <list>
#include <mutex>
#include <chrono>
#include <sstream>
#include <unordered_set>
#include <cctype>
#include <numeric>

namespace transformer {

static float randUniform(std::mt19937 &rng, float lo = -0.08f, float hi = 0.08f) {
	std::uniform_real_distribution<float> dist(lo, hi);
	return dist(rng);
}

Linear::Linear(int in, int out) : w(out, in), b((size_t)out, 0.0f) {
	std::mt19937 rng(0x1234567u);
	for (auto &v : w.data) v = randUniform(rng);
	mW.assign(w.data.size(), 0.0f);
	vW.assign(w.data.size(), 0.0f);
	mB.assign(b.size(), 0.0f);
	vB.assign(b.size(), 0.0f);
	gW.assign(w.data.size(), 0.0f);
	gB.assign(b.size(), 0.0f);
}

std::vector<float> Linear::forward(const std::vector<float> &x) const {
	std::vector<float> out((size_t)w.rows, 0.0f);
	for (int r = 0; r < w.rows; r++) {
		double acc = 0.0;
		for (int c = 0; c < w.cols; c++) acc += w(r, c) * x[(size_t)c];
		acc += b[(size_t)r];
		out[(size_t)r] = (float)acc;
	}
	return out;
}

LayerNorm::LayerNorm(int d)
	: gamma((size_t)d, 1.0f), beta((size_t)d, 0.0f), mGamma((size_t)d, 0.0f), vGamma((size_t)d, 0.0f),
	  mBeta((size_t)d, 0.0f), vBeta((size_t)d, 0.0f), gGamma((size_t)d, 0.0f), gBeta((size_t)d, 0.0f) {}

std::vector<float> LayerNorm::forward(const std::vector<float> &x) const {
	double mean = 0.0;
	for (auto v : x) mean += v;
	mean /= std::max<size_t>(1, x.size());
	double var = 0.0;
	for (auto v : x) { double d = v - mean; var += d * d; }
	var /= std::max<size_t>(1, x.size());
	double denom = 1.0 / std::sqrt(var + eps);
	std::vector<float> out(x.size(), 0.0f);
	for (size_t i = 0; i < x.size(); i++) {
		out[i] = (float)(((x[i] - mean) * denom) * gamma[i] + beta[i]);
	}
	return out;
}

MultiHeadAttention::MultiHeadAttention(int d, int h)
	: nHeads(h), dModel(d), dHead(d / h), wq(d, d), wk(d, d), wv(d, d), wo(d, d) {}

static std::vector<float> softmax(const std::vector<float> &x) {
	float maxv = -1e9f;
	for (float v : x) maxv = std::max(maxv, v);
	double sum = 0.0;
	std::vector<float> out(x.size(), 0.0f);
	for (size_t i = 0; i < x.size(); i++) {
		double v = std::exp((double)x[i] - maxv);
		out[i] = (float)v;
		sum += v;
	}
	if (sum <= 0) return out;
	for (auto &v : out) v = (float)(v / sum);
	return out;
}

static float clipGrad(float g, float clip) {
	if (clip <= 0.0f) return g;
	if (g > clip) return clip;
	if (g < -clip) return -clip;
	return g;
}

static void adamUpdate(float &param, float grad, float &m, float &v, int t, const TransformerParams &p) {
	float g = grad;
	if (p.weightDecay > 0.0f) g += p.weightDecay * param;
	m = p.adamBeta1 * m + (1.0f - p.adamBeta1) * g;
	v = p.adamBeta2 * v + (1.0f - p.adamBeta2) * g * g;
	float mHat = m / (1.0f - std::pow(p.adamBeta1, (float)t));
	float vHat = v / (1.0f - std::pow(p.adamBeta2, (float)t));
	param -= p.lr * mHat / (std::sqrt(vHat) + p.adamEps);
}

static float scheduledLr(int step, const TransformerParams &p) {
	float base = p.lr;
	if (!p.useLrSchedule) return base;
	int warm = std::max(1, p.lrWarmupSteps);
	float lrWarm = (step < warm) ? base * ((float)step / (float)warm) : base;
	int cosineSteps = std::max(0, p.lrCosineSteps);
	if (cosineSteps <= 0) return lrWarm;
	int t = std::max(0, step - warm);
	float progress = (float)(t % cosineSteps) / (float)std::max(1, cosineSteps);
	float cosine = 0.5f * (1.0f + std::cos(3.1415926f * progress));
	float minRatio = std::max(0.0f, std::min(1.0f, p.lrMinRatio));
	float minLr = base * minRatio;
	return minLr + (lrWarm - minLr) * cosine;
}

static std::vector<float> linearBackwardAccumulate(Linear &layer,
									const std::vector<float> &x,
									const std::vector<float> &gradOut,
									const TransformerParams &p) {
	std::vector<float> gradX((size_t)layer.w.cols, 0.0f);
	for (int r = 0; r < layer.w.rows; r++) {
		float g = (r < (int)gradOut.size()) ? gradOut[(size_t)r] : 0.0f;
		g = clipGrad(g, p.gradClip);
		layer.gB[(size_t)r] += g;
		for (int c = 0; c < layer.w.cols; c++) {
			float gw = g * x[(size_t)c];
			gw = clipGrad(gw, p.gradClip);
			size_t idx = (size_t)r * (size_t)layer.w.cols + (size_t)c;
			layer.gW[idx] += gw;
			gradX[(size_t)c] += layer.w(r, c) * g;
		}
	}
	return gradX;
}

static void linearApplyAccum(Linear &layer, const TransformerParams &p) {
	int accum = std::max(1, p.gradAccumSteps);
	layer.step += 1;
	for (int r = 0; r < layer.w.rows; r++) {
		float gb = layer.gB[(size_t)r] / (float)accum;
		adamUpdate(layer.b[(size_t)r], gb, layer.mB[(size_t)r], layer.vB[(size_t)r], layer.step, p);
		layer.gB[(size_t)r] = 0.0f;
		for (int c = 0; c < layer.w.cols; c++) {
			size_t idx = (size_t)r * (size_t)layer.w.cols + (size_t)c;
			float gw = layer.gW[idx] / (float)accum;
			adamUpdate(layer.w(r, c), gw, layer.mW[idx], layer.vW[idx], layer.step, p);
			layer.gW[idx] = 0.0f;
		}
	}
}

struct LNCache {
	std::vector<std::vector<float>> x;
	std::vector<float> mean;
	std::vector<float> invStd;
};

static std::vector<float> layerNormForwardToken(const std::vector<float> &x, const LayerNorm &ln, float &mean, float &invStd) {
	mean = 0.0f;
	for (float v : x) mean += v;
	mean /= (float)std::max<size_t>(1, x.size());
	float var = 0.0f;
	for (float v : x) { float d = v - mean; var += d * d; }
	var /= (float)std::max<size_t>(1, x.size());
	invStd = 1.0f / std::sqrt(var + ln.eps);
	std::vector<float> out(x.size(), 0.0f);
	for (size_t i = 0; i < x.size(); i++) {
		float xhat = (x[i] - mean) * invStd;
		out[i] = xhat * ln.gamma[i] + ln.beta[i];
	}
	return out;
}

static std::vector<float> layerNormBackwardToken(LayerNorm &ln,
										const std::vector<float> &x,
										const std::vector<float> &dy,
										float mean,
										float invStd,
										const TransformerParams &p) {
	int N = (int)x.size();
	std::vector<float> dx(x.size(), 0.0f);
	float sumDy = 0.0f;
	float sumDyX = 0.0f;
	std::vector<float> xhat(x.size(), 0.0f);
	for (int i = 0; i < N; i++) {
		xhat[(size_t)i] = (x[(size_t)i] - mean) * invStd;
		float dyg = dy[(size_t)i] * ln.gamma[(size_t)i];
		sumDy += dyg;
		sumDyX += dyg * xhat[(size_t)i];
	}
	for (int i = 0; i < N; i++) {
		float dyg = dy[(size_t)i] * ln.gamma[(size_t)i];
		dx[(size_t)i] = (1.0f / (float)N) * invStd * ((float)N * dyg - sumDy - xhat[(size_t)i] * sumDyX);
		float dgamma = dy[(size_t)i] * xhat[(size_t)i];
		float dbeta = dy[(size_t)i];
		ln.gGamma[(size_t)i] += clipGrad(dgamma, p.gradClip);
		ln.gBeta[(size_t)i] += clipGrad(dbeta, p.gradClip);
	}
	return dx;
}

static void layerNormApplyAccum(LayerNorm &ln, const TransformerParams &p) {
	int accum = std::max(1, p.gradAccumSteps);
	ln.step += 1;
	for (size_t i = 0; i < ln.gamma.size(); i++) {
		float gGamma = ln.gGamma[i] / (float)accum;
		float gBeta = ln.gBeta[i] / (float)accum;
		adamUpdate(ln.gamma[i], gGamma, ln.mGamma[i], ln.vGamma[i], ln.step, p);
		adamUpdate(ln.beta[i], gBeta, ln.mBeta[i], ln.vBeta[i], ln.step, p);
		ln.gGamma[i] = 0.0f;
		ln.gBeta[i] = 0.0f;
	}
}

static std::vector<std::vector<float>> layerNormForwardBatch(const std::vector<std::vector<float>> &x, const LayerNorm &ln, LNCache &cache) {
	cache.x = x;
	cache.mean.assign(x.size(), 0.0f);
	cache.invStd.assign(x.size(), 0.0f);
	std::vector<std::vector<float>> out(x.size());
	for (size_t i = 0; i < x.size(); i++) {
		out[i] = layerNormForwardToken(x[i], ln, cache.mean[i], cache.invStd[i]);
	}
	return out;
}

static std::vector<std::vector<float>> layerNormBackwardBatch(LayerNorm &ln, const LNCache &cache,
															  const std::vector<std::vector<float>> &dy, const TransformerParams &p) {
	std::vector<std::vector<float>> dx(dy.size());
	for (size_t i = 0; i < dy.size(); i++) {
		dx[i] = layerNormBackwardToken(ln, cache.x[i], dy[i], cache.mean[i], cache.invStd[i], p);
	}
	return dx;
}

struct FFNCache {
	std::vector<std::vector<float>> x;
	std::vector<std::vector<float>> h;
	std::vector<std::vector<float>> hAct;
};

static float geluGrad(float x) {
	float t = std::tanh(0.79788456f * (x + 0.044715f * x * x * x));
	float dt = 0.79788456f * (1 + 3 * 0.044715f * x * x) * (1 - t * t);
	return 0.5f * (1.0f + t) + 0.5f * x * dt;
}

// forward declaration: defined later but used in ffnForward
static float geluFn(float x);

static std::vector<std::vector<float>> ffnForward(const std::vector<std::vector<float>> &x, FeedForward &ffn, FFNCache &cache) {
	cache.x = x;
	cache.h.resize(x.size());
	cache.hAct.resize(x.size());
	std::vector<std::vector<float>> out(x.size());
	for (size_t i = 0; i < x.size(); i++) {
		cache.h[i] = ffn.w1.forward(x[i]);
		cache.hAct[i] = cache.h[i];
		for (auto &v : cache.hAct[i]) v = geluFn(v);
		out[i] = ffn.w2.forward(cache.hAct[i]);
	}
	return out;
}

static std::vector<std::vector<float>> ffnBackward(const std::vector<std::vector<float>> &dOut, FeedForward &ffn,
															const FFNCache &cache, const TransformerParams &p) {
	std::vector<std::vector<float>> dX(dOut.size());
	for (size_t i = 0; i < dOut.size(); i++) {
		auto dHAct = linearBackwardAccumulate(ffn.w2, cache.hAct[i], dOut[i], p);
		std::vector<float> dH(dHAct.size(), 0.0f);
		for (size_t k = 0; k < dHAct.size(); k++) dH[k] = dHAct[k] * geluGrad(cache.h[i][k]);
		dX[i] = linearBackwardAccumulate(ffn.w1, cache.x[i], dH, p);
	}
	return dX;
}

struct AttnCache {
	std::vector<std::vector<float>> x;
	std::vector<std::vector<float>> kSrc;
	std::vector<std::vector<float>> q;
	std::vector<std::vector<float>> k;
	std::vector<std::vector<float>> v;
	std::vector<std::vector<float>> merged;
	std::vector<std::vector<std::vector<float>>> qh;
	std::vector<std::vector<std::vector<float>>> kh;
	std::vector<std::vector<std::vector<float>>> vh;
	std::vector<std::vector<std::vector<float>>> probs;
	bool causal{false};
};

struct KVCache {
	std::vector<std::vector<float>> k;
	std::vector<std::vector<float>> v;
};

static std::vector<std::vector<float>> attnForward(const MultiHeadAttention &att,
											const std::vector<std::vector<float>> &x,
											bool causal,
									const std::vector<std::vector<float>> *memory,
									AttnCache &cache,
									int chunkSize) {
	cache.x = x;
	cache.causal = causal;
	const auto &kSrc = memory ? *memory : x;
	const auto &vSrc = memory ? *memory : x;
	cache.kSrc = kSrc;
	size_t T = x.size();
	size_t S = kSrc.size();
	cache.q.resize(T);
	cache.k.resize(S);
	cache.v.resize(S);
	for (size_t t = 0; t < T; t++) cache.q[t] = att.wq.forward(x[t]);
	for (size_t s = 0; s < S; s++) {
		cache.k[s] = att.wk.forward(kSrc[s]);
		cache.v[s] = att.wv.forward(vSrc[s]);
	}
	int H = att.nHeads;
	int dHead = att.dHead;
	cache.qh.assign((size_t)H, std::vector<std::vector<float>>(T, std::vector<float>((size_t)dHead, 0.0f)));
	cache.kh.assign((size_t)H, std::vector<std::vector<float>>(S, std::vector<float>((size_t)dHead, 0.0f)));
	cache.vh.assign((size_t)H, std::vector<std::vector<float>>(S, std::vector<float>((size_t)dHead, 0.0f)));
	for (int h = 0; h < H; h++) {
		for (size_t t = 0; t < T; t++) {
			for (int i = 0; i < dHead; i++) cache.qh[(size_t)h][t][(size_t)i] = cache.q[t][(size_t)(h * dHead + i)];
		}
		for (size_t s = 0; s < S; s++) {
			for (int i = 0; i < dHead; i++) {
				cache.kh[(size_t)h][s][(size_t)i] = cache.k[s][(size_t)(h * dHead + i)];
				cache.vh[(size_t)h][s][(size_t)i] = cache.v[s][(size_t)(h * dHead + i)];
			}
		}
	}
	cache.probs.assign((size_t)H, std::vector<std::vector<float>>(T, std::vector<float>(S, 0.0f)));
	std::vector<std::vector<float>> merged(T, std::vector<float>((size_t)att.dModel, 0.0f));
	for (int h = 0; h < H; h++) {
		for (size_t t = 0; t < T; t++) {
			size_t sStart = 0;
			size_t sEnd = S;
			if (chunkSize > 0) {
				if (causal && !memory) {
					size_t tEnd = t + 1;
					sEnd = std::min(S, tEnd);
					sStart = (tEnd > (size_t)chunkSize) ? (tEnd - (size_t)chunkSize) : 0;
				} else {
					sStart = (S > (size_t)chunkSize) ? (S - (size_t)chunkSize) : 0;
					sEnd = S;
				}
			}
			std::vector<float> scores(sEnd - sStart, 0.0f);
			for (size_t s = sStart; s < sEnd; s++) {
				double acc = 0.0;
				for (int i = 0; i < dHead; i++) acc += cache.qh[(size_t)h][t][(size_t)i] * cache.kh[(size_t)h][s][(size_t)i];
				if (causal && !memory && s > t) acc = -1e9;
				scores[s - sStart] = (float)(acc / std::sqrt((double)dHead));
			}
			auto probsLocal = softmax(scores);
			for (size_t s = sStart; s < sEnd; s++) cache.probs[(size_t)h][t][s] = probsLocal[s - sStart];
			std::vector<float> head((size_t)dHead, 0.0f);
			for (size_t s = sStart; s < sEnd; s++) {
				float p = probsLocal[s - sStart];
				for (int i = 0; i < dHead; i++) head[(size_t)i] += p * cache.vh[(size_t)h][s][(size_t)i];
			}
			for (int i = 0; i < dHead; i++) merged[t][(size_t)(h * dHead + i)] = head[(size_t)i];
		}
	}
	cache.merged = merged;
	std::vector<std::vector<float>> out(T, std::vector<float>((size_t)att.dModel, 0.0f));
	for (size_t t = 0; t < T; t++) out[t] = att.wo.forward(merged[t]);
	return out;
}

static std::vector<float> attnForwardStep(const MultiHeadAttention &att,
									const std::vector<float> &x,
									KVCache &cache,
									bool appendKV,
									int chunkSize) {
	if (appendKV) {
		cache.k.push_back(att.wk.forward(x));
		cache.v.push_back(att.wv.forward(x));
	}
	auto q = att.wq.forward(x);
	size_t S = cache.k.size();
	int H = att.nHeads;
	int dHead = att.dHead;
	std::vector<float> merged((size_t)att.dModel, 0.0f);
	if (S == 0) return att.wo.forward(merged);
	size_t sStart = 0;
	if (chunkSize > 0 && S > (size_t)chunkSize) sStart = S - (size_t)chunkSize;
	for (int h = 0; h < H; h++) {
		std::vector<float> scores(S - sStart, 0.0f);
		for (size_t s = sStart; s < S; s++) {
			double acc = 0.0;
			for (int i = 0; i < dHead; i++) {
				acc += q[(size_t)(h * dHead + i)] * cache.k[s][(size_t)(h * dHead + i)];
			}
			scores[s - sStart] = (float)(acc / std::sqrt((double)dHead));
		}
		auto probs = softmax(scores);
		std::vector<float> head((size_t)dHead, 0.0f);
		for (size_t s = sStart; s < S; s++) {
			float p = probs[s - sStart];
			for (int i = 0; i < dHead; i++) {
				head[(size_t)i] += p * cache.v[s][(size_t)(h * dHead + i)];
			}
		}
		for (int i = 0; i < dHead; i++) merged[(size_t)(h * dHead + i)] = head[(size_t)i];
	}
	return att.wo.forward(merged);
}

static std::pair<std::vector<std::vector<float>>, std::vector<std::vector<float>>> attnBackward(
										MultiHeadAttention &att,
										const AttnCache &cache,
										const std::vector<std::vector<float>> &dOut,
										const TransformerParams &p) {
	int H = att.nHeads;
	int dHead = att.dHead;
	size_t T = cache.x.size();
	size_t S = cache.kSrc.size();
	std::vector<std::vector<float>> dMerged(T, std::vector<float>((size_t)att.dModel, 0.0f));
	for (size_t t = 0; t < T; t++) {
		dMerged[t] = linearBackwardAccumulate(att.wo, cache.merged[t], dOut[t], p);
	}
	std::vector<std::vector<float>> dQ(T, std::vector<float>((size_t)att.dModel, 0.0f));
	std::vector<std::vector<float>> dK(S, std::vector<float>((size_t)att.dModel, 0.0f));
	std::vector<std::vector<float>> dV(S, std::vector<float>((size_t)att.dModel, 0.0f));
	for (int h = 0; h < H; h++) {
		std::vector<std::vector<float>> dQh(T, std::vector<float>((size_t)dHead, 0.0f));
		std::vector<std::vector<float>> dKh(S, std::vector<float>((size_t)dHead, 0.0f));
		std::vector<std::vector<float>> dVh(S, std::vector<float>((size_t)dHead, 0.0f));
		std::vector<std::vector<float>> dP(T, std::vector<float>(S, 0.0f));
		for (size_t t = 0; t < T; t++) {
			std::vector<float> dOutHead((size_t)dHead, 0.0f);
			for (int i = 0; i < dHead; i++) dOutHead[(size_t)i] = dMerged[t][(size_t)(h * dHead + i)];
			for (size_t s = 0; s < S; s++) {
				float dot = 0.0f;
				for (int i = 0; i < dHead; i++) dot += dOutHead[(size_t)i] * cache.vh[(size_t)h][s][(size_t)i];
				dP[t][s] += dot;
				for (int i = 0; i < dHead; i++) dVh[s][(size_t)i] += cache.probs[(size_t)h][t][s] * dOutHead[(size_t)i];
			}
		}
		for (size_t t = 0; t < T; t++) {
			float dot = 0.0f;
			for (size_t s = 0; s < S; s++) dot += dP[t][s] * cache.probs[(size_t)h][t][s];
			for (size_t s = 0; s < S; s++) {
				float dScore = (dP[t][s] - dot) * cache.probs[(size_t)h][t][s];
				if (cache.causal && s > t) dScore = 0.0f;
				dScore /= std::sqrt((float)dHead);
				for (int i = 0; i < dHead; i++) {
					dQh[t][(size_t)i] += dScore * cache.kh[(size_t)h][s][(size_t)i];
					dKh[s][(size_t)i] += dScore * cache.qh[(size_t)h][t][(size_t)i];
				}
			}
		}
		for (size_t t = 0; t < T; t++) {
			for (int i = 0; i < dHead; i++) dQ[t][(size_t)(h * dHead + i)] += dQh[t][(size_t)i];
		}
		for (size_t s = 0; s < S; s++) {
			for (int i = 0; i < dHead; i++) {
				dK[s][(size_t)(h * dHead + i)] += dKh[s][(size_t)i];
				dV[s][(size_t)(h * dHead + i)] += dVh[s][(size_t)i];
			}
		}
	}
	std::vector<std::vector<float>> dX(T, std::vector<float>((size_t)att.dModel, 0.0f));
	std::vector<std::vector<float>> dMem(S, std::vector<float>((size_t)att.dModel, 0.0f));
	for (size_t t = 0; t < T; t++) {
		auto dx = linearBackwardAccumulate(att.wq, cache.x[t], dQ[t], p);
		for (size_t i = 0; i < dx.size(); i++) dX[t][i] += dx[i];
	}
	for (size_t s = 0; s < S; s++) {
		auto dks = linearBackwardAccumulate(att.wk, cache.kSrc[s], dK[s], p);
		auto dvs = linearBackwardAccumulate(att.wv, cache.kSrc[s], dV[s], p);
		for (size_t i = 0; i < dks.size(); i++) dMem[s][i] += dks[i] + dvs[i];
	}
	return {dX, dMem};
}

static int64_t nowMs() {
	return (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count();
}

static std::string normalizeGraphText(const std::string &text, size_t maxTokens = 512) {
	auto isCjk = [](uint32_t cp) {
		return cp >= 0x4e00 && cp <= 0x9fff;
	};
	auto decode = [](const std::string &s, size_t i, uint32_t &cp, size_t &len) {
		unsigned char c = (unsigned char)s[i];
		if (c < 0x80) { cp = c; len = 1; return true; }
		if ((c >> 5) == 0x6 && i + 1 < s.size()) {
			cp = ((c & 0x1f) << 6) | (s[i + 1] & 0x3f);
			len = 2; return true;
		}
		if ((c >> 4) == 0xe && i + 2 < s.size()) {
			cp = ((c & 0x0f) << 12) | ((s[i + 1] & 0x3f) << 6) | (s[i + 2] & 0x3f);
			len = 3; return true;
		}
		if ((c >> 3) == 0x1e && i + 3 < s.size()) {
			cp = ((c & 0x07) << 18) | ((s[i + 1] & 0x3f) << 12) | ((s[i + 2] & 0x3f) << 6) | (s[i + 3] & 0x3f);
			len = 4; return true;
		}
		cp = c; len = 1; return false;
	};

	std::vector<std::string> tokens;
	std::string cur;
	auto flush = [&]() {
		if (cur.empty()) return;
		tokens.push_back(cur);
		cur.clear();
	};

	for (size_t i = 0; i < text.size();) {
		uint32_t cp = 0; size_t len = 1;
		decode(text, i, cp, len);
		if (cp == '\r' || cp == '\n' || cp == '\t' || cp == ' ') {
			flush();
			i += len; continue;
		}
		if (cp < 0x80) {
			unsigned char c = (unsigned char)cp;
			if (std::isalnum(c) || c == '_' || c == '-') {
				cur.push_back((char)std::tolower(c));
			} else {
				flush();
			}
			i += len; continue;
		}
		if (isCjk(cp)) {
			cur.append(text.substr(i, len));
		} else {
			flush();
		}
		i += len;
		if (tokens.size() >= maxTokens) break;
	}
	flush();
	std::ostringstream oss;
	for (size_t i = 0; i < tokens.size() && i < maxTokens; i++) {
		if (i) oss << " ";
		oss << tokens[i];
	}
	return oss.str();
}

static std::string hashKey(const std::string &a, const std::string &b, int maxTokens, const TransformerParams &p) {
	std::ostringstream oss;
	oss << a << "|" << b << "|" << maxTokens << "|" << p.graphWeight << "|" << p.temperature
		<< "|" << p.dynamicSampling << "|" << p.temperatureMin << "|" << p.temperatureMax
		<< "|" << p.topK << "|" << p.topP << "|" << p.topPMin << "|" << p.topPMax
		<< "|" << p.dynamicWindow << "|" << p.dynamicRepetitionBoost
		<< "|" << p.repetitionPenalty << "|" << p.repetitionSegmented
		<< "|" << p.repetitionWindowRecent << "|" << p.repetitionWindowMid
		<< "|" << p.repetitionPenaltyRecent << "|" << p.repetitionPenaltyMid << "|" << p.repetitionPenaltyOld
		<< "|" << p.noRepeatNgram << "|" << p.minNewTokens << "|" << p.bidirectionalWeight
		<< "|" << p.selfFlowWeight << "|" << p.attnChunkSize << "|" << p.tokenizerMode;
	std::string s = oss.str();
	uint32_t h = 2166136261u;
	for (unsigned char c : s) { h ^= c; h *= 16777619u; }
	return std::to_string(h);
}

static std::vector<int> reverseTokensPreserveSpecial(const std::vector<int> &tokens) {
	if (tokens.size() <= 2) return tokens;
	std::vector<int> out = tokens;
	int start = (tokens.front() == 1) ? 1 : 0;
	int end = (tokens.back() == 2) ? (int)tokens.size() - 1 : (int)tokens.size();
	std::reverse(out.begin() + start, out.begin() + end);
	return out;
}

static std::vector<std::vector<float>> applySelfFlow(const std::vector<std::vector<float>> &mem, float weight) {
	if (mem.empty() || weight <= 0.0f) return mem;
	std::vector<std::vector<float>> out = mem;
	for (size_t i = 0; i < mem.size(); i++) {
		const auto &cur = mem[i];
		const auto &prev = (i > 0) ? mem[i - 1] : cur;
		const auto &next = (i + 1 < mem.size()) ? mem[i + 1] : cur;
		for (size_t d = 0; d < cur.size(); d++) {
			float smooth = (prev[d] + cur[d] + next[d]) / 3.0f;
			out[i][d] = cur[d] + weight * (smooth - cur[d]);
		}
	}
	return out;
}

static float l2norm(const std::vector<float> &v) {
	double acc = 0.0;
	for (float x : v) acc += (double)x * (double)x;
	return (float)std::sqrt(acc);
}

static float avgNorm(const std::vector<std::vector<float>> &m) {
	if (m.empty()) return 0.0f;
	double acc = 0.0;
	for (const auto &v : m) acc += l2norm(v);
	return (float)(acc / std::max<size_t>(1, m.size()));
}

static float sigmoidf(float x) { return 1.0f / (1.0f + std::exp(-x)); }

std::vector<std::vector<float>> MultiHeadAttention::forward(const std::vector<std::vector<float>> &x,
															bool causal,
															const std::vector<std::vector<float>> *memory) const {
	const auto &kSrc = memory ? *memory : x;
	const auto &vSrc = memory ? *memory : x;
	size_t T = x.size();
	size_t S = kSrc.size();
	std::vector<std::vector<float>> out(T, std::vector<float>((size_t)dModel, 0.0f));
	for (size_t t = 0; t < T; t++) {
		auto q = wq.forward(x[t]);
		std::vector<float> merged((size_t)dModel, 0.0f);
		for (int h = 0; h < nHeads; h++) {
			std::vector<float> scores(S, 0.0f);
			for (size_t s = 0; s < S; s++) {
				auto k = wk.forward(kSrc[s]);
				double acc = 0.0;
				for (int i = 0; i < dHead; i++) {
					int idx = h * dHead + i;
					acc += q[(size_t)idx] * k[(size_t)idx];
				}
				if (causal && s > t && !memory) acc = -1e9;
				scores[s] = (float)(acc / std::sqrt((double)dHead));
			}
			auto probs = softmax(scores);
			std::vector<float> head((size_t)dHead, 0.0f);
			for (size_t s = 0; s < S; s++) {
				auto v = wv.forward(vSrc[s]);
				for (int i = 0; i < dHead; i++) {
					int idx = h * dHead + i;
					head[(size_t)i] += probs[s] * v[(size_t)idx];
				}
			}
			for (int i = 0; i < dHead; i++) {
				merged[(size_t)(h * dHead + i)] = head[(size_t)i];
			}
		}
		out[t] = wo.forward(merged);
	}
	return out;
}

FeedForward::FeedForward(int dModel, int dFF) : w1(dModel, dFF), w2(dFF, dModel) {}

static float geluFn(float x) {
	return 0.5f * x * (1.0f + std::tanh(std::sqrt(2.0f / 3.1415926f) * (x + 0.044715f * x * x * x)));
}

std::vector<float> FeedForward::forward(const std::vector<float> &x) const {
	auto h = w1.forward(x);
	for (auto &v : h) v = geluFn(v);
	return w2.forward(h);
}

EncoderLayer::EncoderLayer(int dModel, int nHeads, int dFF)
	: selfAttn(dModel, nHeads), ffn(dModel, dFF), ln1(dModel), ln2(dModel) {}

std::vector<std::vector<float>> EncoderLayer::forward(const std::vector<std::vector<float>> &x) const {
	auto att = selfAttn.forward(x, false, nullptr);
	std::vector<std::vector<float>> y = x;
	for (size_t i = 0; i < x.size(); i++) {
		std::vector<float> sum(x[i].size());
		for (size_t j = 0; j < sum.size(); j++) sum[j] = x[i][j] + att[i][j];
		y[i] = ln1.forward(sum);
	}
	auto z = y;
	for (size_t i = 0; i < y.size(); i++) {
		auto ff = ffn.forward(y[i]);
		std::vector<float> sum(y[i].size());
		for (size_t j = 0; j < sum.size(); j++) sum[j] = y[i][j] + ff[j];
		z[i] = ln2.forward(sum);
	}
	return z;
}

DecoderLayer::DecoderLayer(int dModel, int nHeads, int dFF)
	: selfAttn(dModel, nHeads), crossAttn(dModel, nHeads), ffn(dModel, dFF), ln1(dModel), ln2(dModel), ln3(dModel) {}

std::vector<std::vector<float>> DecoderLayer::forward(const std::vector<std::vector<float>> &x,
													  const std::vector<std::vector<float>> &memory) const {
	auto att = selfAttn.forward(x, true, nullptr);
	std::vector<std::vector<float>> y = x;
	for (size_t i = 0; i < x.size(); i++) {
		std::vector<float> sum(x[i].size());
		for (size_t j = 0; j < sum.size(); j++) sum[j] = x[i][j] + att[i][j];
		y[i] = ln1.forward(sum);
	}
	auto cross = crossAttn.forward(y, false, &memory);
	auto z = y;
	for (size_t i = 0; i < y.size(); i++) {
		std::vector<float> sum(y[i].size());
		for (size_t j = 0; j < sum.size(); j++) sum[j] = y[i][j] + cross[i][j];
		z[i] = ln2.forward(sum);
	}
	auto out = z;
	for (size_t i = 0; i < z.size(); i++) {
		auto ff = ffn.forward(z[i]);
		std::vector<float> sum(z[i].size());
		for (size_t j = 0; j < sum.size(); j++) sum[j] = z[i][j] + ff[j];
		out[i] = ln3.forward(sum);
	}
	return out;
}

Tokenizer::Tokenizer(int vocabSize, const std::string &mode) : vocabSize_(vocabSize), mode_("hash") {
	setMode(mode);
}

void Tokenizer::setMode(const std::string &mode) {
	if (mode == "byte" || mode == "hash") mode_ = mode;
	else mode_ = "hash";
}

std::vector<int> Tokenizer::encode(const std::string &text, int maxLen, bool addBosEos) const {
	std::vector<int> ids;
	ids.reserve((size_t)maxLen);
	if (addBosEos) ids.push_back(1);
	if (mode_ == "byte") {
		for (unsigned char c : text) {
			int id = 4 + (int)c;
			if (id >= vocabSize_) id = 4 + (id % std::max(1, vocabSize_ - 4));
			ids.push_back(id);
			if ((int)ids.size() >= maxLen) break;
		}
		if (addBosEos && (int)ids.size() < maxLen) ids.push_back(2);
		if ((int)ids.size() > maxLen) ids.resize(maxLen);
		return ids;
	}
	std::string cur;
	for (char c : text) {
		if (std::isspace((unsigned char)c)) {
			if (!cur.empty()) {
				uint32_t h = 0x811c9dc5u;
				for (unsigned char x : cur) { h ^= x; h *= 0x01000193u; }
				ids.push_back((int)(h % (uint32_t)(vocabSize_ - 4) + 4));
				cur.clear();
			}
			continue;
		}
		cur.push_back(c);
	}
	if (!cur.empty()) {
		uint32_t h = 0x811c9dc5u;
		for (unsigned char x : cur) { h ^= x; h *= 0x01000193u; }
		ids.push_back((int)(h % (uint32_t)(vocabSize_ - 4) + 4));
	}
	if (addBosEos) ids.push_back(2);
	if ((int)ids.size() > maxLen) ids.resize(maxLen);
	return ids;
}

std::string Tokenizer::decode(const std::vector<int> &ids) const {
	std::ostringstream oss;
	for (size_t i = 0; i < ids.size(); i++) {
		if (ids[i] <= 3) continue;
		if (mode_ == "byte") {
			int byteId = ids[i] - 4;
			if (byteId >= 0 && byteId < 256) {
				oss << (char)byteId;
				continue;
			}
		}
		if (i) oss << " ";
		oss << "tok" << ids[i];
	}
	return oss.str();
}

TransformerModel::TransformerModel(const TransformerParams &params)
	: params_(params), tokEmbed_(params.vocabSize, params.dModel), posEmbed_(params.maxLen, params.dModel),
	  outProj_(params.dModel, params.vocabSize), fuseAttn_(params.dModel, params.nHeads),
	  fuseGate_(params.dModel * 2, params.dModel), tokenizer_(params.vocabSize, params.tokenizerMode) {
	encLayers_.reserve(params.nLayers);
	decLayers_.reserve(params.nLayers);
	for (int i = 0; i < params.nLayers; i++) {
		encLayers_.emplace_back(params.dModel, params.nHeads, params.dFF);
		decLayers_.emplace_back(params.dModel, params.nHeads, params.dFF);
	}
	std::mt19937 rng(0x42u);
	for (auto &v : tokEmbed_.data) v = randUniform(rng);
	for (auto &v : posEmbed_.data) v = randUniform(rng);
	outMom1_.assign(outProj_.w.data.size() + outProj_.b.size(), 0.0f);
	outMom2_.assign(outProj_.w.data.size() + outProj_.b.size(), 0.0f);
	outGradW_.assign(outProj_.w.data.size(), 0.0f);
	outGradB_.assign(outProj_.b.size(), 0.0f);
	tokMom1_.assign(tokEmbed_.data.size(), 0.0f);
	tokMom2_.assign(tokEmbed_.data.size(), 0.0f);
	posMom1_.assign(posEmbed_.data.size(), 0.0f);
	posMom2_.assign(posEmbed_.data.size(), 0.0f);
	tokGrad_.assign(tokEmbed_.data.size(), 0.0f);
	posGrad_.assign(posEmbed_.data.size(), 0.0f);
}

void TransformerModel::updateParams(const TransformerParams &p) {
	params_ = p;
}

std::vector<std::vector<float>> TransformerModel::embedTokens(const std::vector<int> &tokens) const {
	int T = (int)tokens.size();
	std::vector<std::vector<float>> out((size_t)T, std::vector<float>((size_t)params_.dModel, 0.0f));
	for (int t = 0; t < T; t++) {
		int id = std::max(0, std::min(params_.vocabSize - 1, tokens[t]));
		for (int d = 0; d < params_.dModel; d++) {
			float w = tokEmbed_(id, d);
			float p = posEmbed_(t, d);
			out[(size_t)t][(size_t)d] = w + p;
		}
	}
	return out;
}

std::vector<std::vector<float>> TransformerModel::encode(const std::vector<int> &tokens) const {
	auto x = embedTokens(tokens);
	for (auto &layer : encLayers_) x = layer.forward(x);
	return x;
}

std::vector<std::vector<float>> TransformerModel::decode(const std::vector<int> &tokens,
														 const std::vector<std::vector<float>> &memory) const {
	auto x = embedTokens(tokens);
	for (auto &layer : decLayers_) x = layer.forward(x, memory);
	return x;
}

std::vector<float> TransformerModel::logitsAt(const std::vector<float> &hidden) const {
	return outProj_.forward(hidden);
}

std::vector<std::vector<float>> TransformerModel::fuseMemory(const std::vector<std::vector<float>> &textMem,
											const std::vector<std::vector<float>> &graphMem,
											float graphWeight) const {
	if (textMem.empty() || graphMem.empty() || graphWeight <= 0.0f) return textMem;
	float textNorm = avgNorm(textMem);
	float graphNorm = avgNorm(graphMem);
	float ratio = graphNorm / std::max(1e-6f, textNorm);
	float dynWeight = graphWeight * std::max(0.5f, std::min(2.0f, ratio));
	AttnCache cache;
	auto ctx = attnForward(const_cast<MultiHeadAttention &>(fuseAttn_), textMem, false, &graphMem, cache, params_.attnChunkSize);
	std::vector<std::vector<float>> out = textMem;
	for (size_t i = 0; i < textMem.size() && i < ctx.size(); i++) {
		std::vector<float> gateIn;
		gateIn.reserve(textMem[i].size() + ctx[i].size());
		gateIn.insert(gateIn.end(), textMem[i].begin(), textMem[i].end());
		gateIn.insert(gateIn.end(), ctx[i].begin(), ctx[i].end());
		auto gate = fuseGate_.forward(gateIn);
		for (size_t d = 0; d < gate.size(); d++) gate[d] = sigmoidf(gate[d]);
		for (size_t d = 0; d < out[i].size() && d < ctx[i].size() && d < gate.size(); d++) {
			out[i][d] += dynWeight * gate[d] * ctx[i][d];
		}
		// Apply layer normalization to the fused memory
		float normFactor = std::sqrt(std::inner_product(out[i].begin(), out[i].end(), out[i].begin(), 0.0f));
		if (normFactor > 0.0f) {
			for (auto &val : out[i]) val /= normFactor;
		}
	}
	return out;
}

std::vector<int> TransformerModel::generate(const std::vector<int> &inputTokens,
							const std::vector<int> &graphTokens,
					int maxTokens,
					float graphWeight) const {
	std::vector<std::vector<float>> empty;
	return generate(inputTokens, graphTokens, empty, maxTokens, graphWeight);
}

std::vector<int> TransformerModel::generate(const std::vector<int> &inputTokens,
							const std::vector<int> &graphTokens,
					const std::vector<std::vector<float>> &graphEmbeddings,
					int maxTokens,
					float graphWeight) const {
	auto mem = encode(inputTokens);
	if (params_.bidirectionalWeight > 0.0f) {
		auto rev = reverseTokensPreserveSpecial(inputTokens);
		auto memRev = encode(rev);
		for (size_t i = 0; i < mem.size() && i < memRev.size(); i++) {
			for (size_t d = 0; d < mem[i].size(); d++) {
				mem[i][d] = mem[i][d] + params_.bidirectionalWeight * memRev[i][d];
			}
		}
	}
	std::vector<std::vector<float>> gmem;
	if (!graphEmbeddings.empty()) {
		gmem.reserve(graphEmbeddings.size());
		for (const auto &vec : graphEmbeddings) {
			std::vector<float> v((size_t)params_.dModel, 0.0f);
			for (size_t d = 0; d < v.size() && d < vec.size(); d++) v[d] = vec[d];
			float norm = 0.0f;
			for (float x : v) norm += x * x;
			norm = std::sqrt(norm);
			if (norm > 0.0f) for (auto &x : v) x /= norm;
			gmem.push_back(std::move(v));
		}
	} else if (!graphTokens.empty()) {
		gmem = encode(graphTokens);
		if (params_.bidirectionalWeight > 0.0f) {
			auto grev = reverseTokensPreserveSpecial(graphTokens);
			auto gmemRev = encode(grev);
			for (size_t i = 0; i < gmem.size() && i < gmemRev.size(); i++) {
				for (size_t d = 0; d < gmem[i].size(); d++) {
					gmem[i][d] += params_.bidirectionalWeight * gmemRev[i][d];
				}
			}
		}
	}
	if (!gmem.empty()) {
		mem = fuseMemory(mem, gmem, graphWeight);
		if (graphWeight > 0.0f) {
			for (auto &v : gmem) {
				for (auto &x : v) x *= graphWeight;
			}
		}
		mem.insert(mem.end(), gmem.begin(), gmem.end());
	}
	mem = applySelfFlow(mem, params_.selfFlowWeight);
	std::vector<KVCache> selfCaches(decLayers_.size());
	std::vector<KVCache> crossCaches(decLayers_.size());
	for (size_t li = 0; li < decLayers_.size(); li++) {
		auto &layer = decLayers_[li];
		crossCaches[li].k.reserve(mem.size());
		crossCaches[li].v.reserve(mem.size());
		for (size_t s = 0; s < mem.size(); s++) {
			crossCaches[li].k.push_back(layer.crossAttn.wk.forward(mem[s]));
			crossCaches[li].v.push_back(layer.crossAttn.wv.forward(mem[s]));
		}
	}
	std::vector<int> out = {1};
	for (int step = 0; step < maxTokens; step++) {
		int pos = (int)out.size() - 1;
		std::vector<float> x((size_t)params_.dModel, 0.0f);
		int id = std::max(0, std::min(params_.vocabSize - 1, out.back()));
		for (int d = 0; d < params_.dModel; d++) x[(size_t)d] = tokEmbed_(id, d) + posEmbed_(pos, d);
		for (size_t li = 0; li < decLayers_.size(); li++) {
			auto &layer = decLayers_[li];
			auto selfOut = attnForwardStep(layer.selfAttn, x, selfCaches[li], true, params_.attnChunkSize);
			std::vector<float> sum1(x.size(), 0.0f);
			for (size_t d = 0; d < x.size(); d++) sum1[d] = x[d] + selfOut[d];
			float mean1 = 0.0f;
			float inv1 = 1.0f;
			auto y = layerNormForwardToken(sum1, layer.ln1, mean1, inv1);
			auto crossOut = attnForwardStep(layer.crossAttn, y, crossCaches[li], false, params_.attnChunkSize);
			std::vector<float> sum2(y.size(), 0.0f);
			for (size_t d = 0; d < y.size(); d++) sum2[d] = y[d] + crossOut[d];
			float mean2 = 0.0f;
			float inv2 = 1.0f;
			auto z = layerNormForwardToken(sum2, layer.ln2, mean2, inv2);
			auto ff = layer.ffn.forward(z);
			std::vector<float> sum3(z.size(), 0.0f);
			for (size_t d = 0; d < z.size(); d++) sum3[d] = z[d] + ff[d];
			float mean3 = 0.0f;
			float inv3 = 1.0f;
			x = layerNormForwardToken(sum3, layer.ln3, mean3, inv3);
		}
		auto logits = logitsAt(x);
		auto applyPenalty = [&](int id, float penalty) {
			if (penalty <= 1.0f) return;
			if (id >= 0 && id < (int)logits.size()) logits[(size_t)id] /= penalty;
		};
		if (params_.repetitionSegmented) {
			int recentW = std::max(1, params_.repetitionWindowRecent);
			int midW = std::max(recentW + 1, params_.repetitionWindowMid);
			float penRecent = std::max(1.0f, params_.repetitionPenaltyRecent);
			float penMid = std::max(1.0f, params_.repetitionPenaltyMid);
			float penOld = std::max(1.0f, params_.repetitionPenaltyOld);
			std::unordered_set<int> seen;
			for (int i = (int)out.size() - 1; i >= 0; i--) {
				int id = out[(size_t)i];
				if (seen.count(id)) continue;
				seen.insert(id);
				int dist = (int)out.size() - 1 - i;
				float penalty = (dist <= recentW) ? penRecent : (dist <= midW ? penMid : penOld);
				applyPenalty(id, penalty);
			}
		} else if (params_.repetitionPenalty > 1.0f) {
			for (int id : out) applyPenalty(id, params_.repetitionPenalty);
		}
		if (params_.noRepeatNgram >= 2 && (int)out.size() >= params_.noRepeatNgram - 1) {
			int n = params_.noRepeatNgram;
			int prefixLen = n - 1;
			std::vector<int> prefix(out.end() - prefixLen, out.end());
			std::unordered_set<int> banned;
			for (size_t i = 0; i + (size_t)n <= out.size(); i++) {
				bool match = true;
				for (int k = 0; k < prefixLen; k++) {
					if (out[i + (size_t)k] != prefix[(size_t)k]) { match = false; break; }
				}
				if (match) banned.insert(out[i + (size_t)prefixLen]);
			}
			for (int id : banned) {
				if (id >= 0 && id < (int)logits.size()) logits[(size_t)id] = -1e9f;
			}
		}
		float temp = params_.temperature;
		float topP = params_.topP;
		if (params_.dynamicSampling) {
			float progress = 1.0f;
			if (maxTokens > 1) {
				progress = (float)std::max(0, (int)out.size() - 1) / (float)std::max(1, maxTokens - 1);
				progress = std::max(0.0f, std::min(1.0f, progress));
			}
			float tMin = (params_.temperatureMin > 0.0f) ? params_.temperatureMin : temp;
			float tMax = (params_.temperatureMax > 0.0f) ? params_.temperatureMax : temp;
			float pMin = (params_.topPMin > 0.0f) ? params_.topPMin : topP;
			float pMax = (params_.topPMax > 0.0f) ? params_.topPMax : topP;
			int win = std::max(2, params_.dynamicWindow);
			size_t start = out.size() > (size_t)win ? out.size() - (size_t)win : 0;
			std::unordered_set<int> uniq;
			for (size_t i = start; i < out.size(); i++) uniq.insert(out[i]);
			float uniqRatio = (float)uniq.size() / (float)std::max<size_t>(1, out.size() - start);
			float repRatio = std::max(0.0f, std::min(1.0f, 1.0f - uniqRatio));
			float boost = std::max(0.0f, params_.dynamicRepetitionBoost) * repRatio;
			temp = tMin + (tMax - tMin) * progress;
			temp *= (1.0f + boost);
			topP = pMin + (pMax - pMin) * progress;
			topP *= (1.0f + boost);
		}
		temp = std::max(0.1f, temp);
		for (auto &v : logits) v /= temp;
		std::vector<int> idx(logits.size());
		for (size_t i = 0; i < idx.size(); i++) idx[i] = (int)i;
		std::partial_sort(idx.begin(), idx.begin() + std::min<int>((int)idx.size(), std::max(1, params_.topK)), idx.end(),
			[&](int a, int b){ return logits[a] > logits[b]; });
		std::vector<float> probs = softmax(logits);
		if (params_.topK > 0 && params_.topK < (int)probs.size()) {
			std::vector<float> keep(probs.size(), 0.0f);
			for (int i = 0; i < params_.topK; i++) keep[(size_t)idx[i]] = probs[(size_t)idx[i]];
			probs = softmax(keep);
		}
		if (topP > 0.0f && topP < 1.0f) {
			std::vector<int> order(idx.begin(), idx.end());
			std::vector<float> keep(probs.size(), 0.0f);
			double cum = 0.0;
			for (int id : order) {
				cum += probs[(size_t)id];
				keep[(size_t)id] = probs[(size_t)id];
				if (cum >= topP) break;
			}
			probs = softmax(keep);
		}
		static thread_local std::mt19937 rng{std::random_device{}()};
		int next = 0;
		float sum = 0.0f;
		for (float v : probs) sum += v;
		if (sum <= 0.0f) {
			float bestv = -1e9f;
			for (int i = 0; i < (int)logits.size(); i++) {
				if (logits[i] > bestv) { bestv = logits[i]; next = i; }
			}
		} else {
			std::discrete_distribution<int> dist(probs.begin(), probs.end());
			next = dist(rng);
		}
		out.push_back(next);
		int minNew = std::max(0, params_.minNewTokens);
		if (next == 2 && (int)out.size() - 1 >= minNew) break;
	}
	return out;
}

float TransformerModel::trainOnSample(const std::vector<int> &inputTokens,
									  const std::vector<int> &graphTokens,
									  const std::vector<int> &targetTokens,
									  float lr) {
	if (lr > 0.0f) params_.lr = lr;
	trainStep_ += 1;
	TransformerParams p = params_;
	p.lr = scheduledLr(trainStep_, params_);
	int accumSteps = std::max(1, p.gradAccumSteps);
	bool applyUpdate = (trainStep_ % accumSteps == 0);
	auto embedForward = [&](const std::vector<int> &tokens) {
		std::vector<std::vector<float>> out;
		out.reserve(tokens.size());
		for (size_t t = 0; t < tokens.size(); t++) {
			int id = std::max(0, std::min(p.vocabSize - 1, tokens[t]));
			std::vector<float> v((size_t)p.dModel, 0.0f);
			for (int d = 0; d < p.dModel; d++) v[(size_t)d] = tokEmbed_(id, d) + posEmbed_((int)t, d);
			out.push_back(std::move(v));
		}
		return out;
	};
	auto applyEmbeddingGrad = [&](const std::vector<int> &tokens, const std::vector<std::vector<float>> &dEmb) {
		for (size_t t = 0; t < tokens.size() && t < dEmb.size(); t++) {
			int id = std::max(0, std::min(p.vocabSize - 1, tokens[t]));
			for (int d = 0; d < p.dModel; d++) {
				float g = clipGrad(dEmb[t][(size_t)d], p.gradClip);
				size_t tokIdx = (size_t)id * (size_t)p.dModel + (size_t)d;
				size_t posIdx = (size_t)t * (size_t)p.dModel + (size_t)d;
				tokGrad_[tokIdx] += g;
				posGrad_[posIdx] += g;
			}
		}
	};

	struct EncCache {
		AttnCache attn;
		LNCache ln1;
		LNCache ln2;
		FFNCache ffn;
		std::vector<std::vector<float>> ln1_in;
		std::vector<std::vector<float>> ln2_in;
	};
	struct DecCache {
		AttnCache self;
		LNCache ln1;
		AttnCache cross;
		LNCache ln2;
		FFNCache ffn;
		LNCache ln3;
		std::vector<std::vector<float>> ln1_in;
		std::vector<std::vector<float>> ln2_in;
		std::vector<std::vector<float>> ln3_in;
	};

	auto encodeWithCache = [&](const std::vector<int> &tokens, std::vector<EncCache> &caches) {
		std::vector<std::vector<float>> x = embedForward(tokens);
		caches.clear();
		caches.reserve(encLayers_.size());
		for (auto &layer : encLayers_) {
			EncCache cache;
			auto att = attnForward(layer.selfAttn, x, false, nullptr, cache.attn, p.attnChunkSize);
			cache.ln1_in.resize(x.size());
			for (size_t i = 0; i < x.size(); i++) {
				cache.ln1_in[i].resize(x[i].size());
				for (size_t d = 0; d < x[i].size(); d++) cache.ln1_in[i][d] = x[i][d] + att[i][d];
			}
			auto y = layerNormForwardBatch(cache.ln1_in, layer.ln1, cache.ln1);
			auto ff = ffnForward(y, layer.ffn, cache.ffn);
			cache.ln2_in.resize(y.size());
			for (size_t i = 0; i < y.size(); i++) {
				cache.ln2_in[i].resize(y[i].size());
				for (size_t d = 0; d < y[i].size(); d++) cache.ln2_in[i][d] = y[i][d] + ff[i][d];
			}
			x = layerNormForwardBatch(cache.ln2_in, layer.ln2, cache.ln2);
			caches.push_back(std::move(cache));
		}
		return x;
	};

	auto decodeWithCache = [&](const std::vector<int> &tokens, const std::vector<std::vector<float>> &mem,
							std::vector<DecCache> &caches) {
		std::vector<std::vector<float>> x = embedForward(tokens);
		caches.clear();
		caches.reserve(decLayers_.size());
		for (auto &layer : decLayers_) {
			DecCache cache;
			auto self = attnForward(layer.selfAttn, x, true, nullptr, cache.self, p.attnChunkSize);
			cache.ln1_in.resize(x.size());
			for (size_t i = 0; i < x.size(); i++) {
				cache.ln1_in[i].resize(x[i].size());
				for (size_t d = 0; d < x[i].size(); d++) cache.ln1_in[i][d] = x[i][d] + self[i][d];
			}
			auto y = layerNormForwardBatch(cache.ln1_in, layer.ln1, cache.ln1);
			auto cross = attnForward(layer.crossAttn, y, false, &mem, cache.cross, p.attnChunkSize);
			cache.ln2_in.resize(y.size());
			for (size_t i = 0; i < y.size(); i++) {
				cache.ln2_in[i].resize(y[i].size());
				for (size_t d = 0; d < y[i].size(); d++) cache.ln2_in[i][d] = y[i][d] + cross[i][d];
			}
			auto z = layerNormForwardBatch(cache.ln2_in, layer.ln2, cache.ln2);
			auto ff = ffnForward(z, layer.ffn, cache.ffn);
			cache.ln3_in.resize(z.size());
			for (size_t i = 0; i < z.size(); i++) {
				cache.ln3_in[i].resize(z[i].size());
				for (size_t d = 0; d < z[i].size(); d++) cache.ln3_in[i][d] = z[i][d] + ff[i][d];
			}
			x = layerNormForwardBatch(cache.ln3_in, layer.ln3, cache.ln3);
			caches.push_back(std::move(cache));
		}
		return x;
	};

	std::vector<EncCache> encCacheText;
	std::vector<EncCache> encCacheTextRev;
	std::vector<EncCache> encCacheGraph;
	std::vector<EncCache> encCacheGraphRev;
	std::vector<std::vector<float>> memText = encodeWithCache(inputTokens, encCacheText);
	if (p.bidirectionalWeight > 0.0f) {
		auto rev = reverseTokensPreserveSpecial(inputTokens);
		auto memRev = encodeWithCache(rev, encCacheTextRev);
		for (size_t i = 0; i < memText.size() && i < memRev.size(); i++) {
			for (size_t d = 0; d < memText[i].size(); d++) memText[i][d] += p.bidirectionalWeight * memRev[i][d];
		}
	}
	std::vector<std::vector<float>> memGraph;
	if (!graphTokens.empty()) {
		memGraph = encodeWithCache(graphTokens, encCacheGraph);
		if (p.bidirectionalWeight > 0.0f) {
			auto grev = reverseTokensPreserveSpecial(graphTokens);
			auto memGRev = encodeWithCache(grev, encCacheGraphRev);
			for (size_t i = 0; i < memGraph.size() && i < memGRev.size(); i++) {
				for (size_t d = 0; d < memGraph[i].size(); d++) memGraph[i][d] += p.bidirectionalWeight * memGRev[i][d];
			}
		}
	}

	struct FuseCache {
		AttnCache attn;
		std::vector<std::vector<float>> ctx;
		std::vector<std::vector<float>> gateIn;
		std::vector<std::vector<float>> gate;
		std::vector<std::vector<float>> graph;
		float weight{0.0f};
	};
	auto fuseMemoryTrain = [&](const std::vector<std::vector<float>> &text,
									const std::vector<std::vector<float>> &graph,
									float weight,
									FuseCache &cache) {
		cache.graph = graph;
		float textNorm = avgNorm(text);
		float graphNorm = avgNorm(graph);
		float ratio = graphNorm / std::max(1e-6f, textNorm);
		cache.weight = weight * std::max(0.5f, std::min(2.0f, ratio));
		cache.ctx = attnForward(fuseAttn_, text, false, &graph, cache.attn, p.attnChunkSize);
		cache.gateIn.assign(text.size(), {});
		cache.gate.assign(text.size(), {});
		std::vector<std::vector<float>> out = text;
		for (size_t i = 0; i < text.size() && i < cache.ctx.size(); i++) {
			cache.gateIn[i].reserve(text[i].size() + cache.ctx[i].size());
			cache.gateIn[i].insert(cache.gateIn[i].end(), text[i].begin(), text[i].end());
			cache.gateIn[i].insert(cache.gateIn[i].end(), cache.ctx[i].begin(), cache.ctx[i].end());
			auto gatePre = fuseGate_.forward(cache.gateIn[i]);
			cache.gate[i] = gatePre;
			for (auto &v : cache.gate[i]) v = sigmoidf(v);
			for (size_t d = 0; d < out[i].size() && d < cache.ctx[i].size() && d < cache.gate[i].size(); d++) {
				out[i][d] += cache.weight * cache.gate[i][d] * cache.ctx[i][d];
			}
		}
		return out;
	};
	auto fuseMemoryBackward = [&](const FuseCache &cache,
									const std::vector<std::vector<float>> &dOut,
								float weight) {
		(void)weight;
		std::vector<std::vector<float>> dText = dOut;
		std::vector<std::vector<float>> dCtx(cache.ctx.size(), std::vector<float>((size_t)p.dModel, 0.0f));
		std::vector<std::vector<float>> dGate(cache.ctx.size(), std::vector<float>((size_t)p.dModel, 0.0f));
		for (size_t i = 0; i < dOut.size() && i < cache.ctx.size(); i++) {
			for (size_t d = 0; d < dOut[i].size() && d < cache.ctx[i].size() && d < cache.gate[i].size(); d++) {
				dCtx[i][d] += dOut[i][d] * cache.weight * cache.gate[i][d];
				dGate[i][d] += dOut[i][d] * cache.weight * cache.ctx[i][d];
			}
		}
		for (size_t i = 0; i < dGate.size(); i++) {
			std::vector<float> dGatePre(dGate[i].size(), 0.0f);
			for (size_t d = 0; d < dGate[i].size(); d++) {
				float g = cache.gate[i][d];
				dGatePre[d] = dGate[i][d] * g * (1.0f - g);
			}
			auto dGateIn = linearBackwardAccumulate(fuseGate_, cache.gateIn[i], dGatePre, p);
			for (size_t d = 0; d < (size_t)p.dModel && d < dGateIn.size(); d++) dText[i][d] += dGateIn[d];
			for (size_t d = 0; d < (size_t)p.dModel && (d + (size_t)p.dModel) < dGateIn.size(); d++) {
				dCtx[i][d] += dGateIn[d + (size_t)p.dModel];
			}
		}
		auto attnBack = attnBackward(fuseAttn_, cache.attn, dCtx, p);
		for (size_t i = 0; i < dText.size() && i < attnBack.first.size(); i++) {
			for (size_t d = 0; d < dText[i].size(); d++) dText[i][d] += attnBack.first[i][d];
		}
		std::vector<std::vector<float>> dGraph = attnBack.second;
		return std::make_pair(dText, dGraph);
	};

	FuseCache fuseCache;
	std::vector<std::vector<float>> mem = memText;
	bool fused = false;
	if (!memGraph.empty() && p.graphWeight > 0.0f) {
		mem = fuseMemoryTrain(memText, memGraph, p.graphWeight, fuseCache);
		fused = true;
		mem.insert(mem.end(), memGraph.begin(), memGraph.end());
	} else if (!memGraph.empty()) {
		mem.insert(mem.end(), memGraph.begin(), memGraph.end());
	}
	auto memBeforeSelf = mem;
	mem = applySelfFlow(mem, p.selfFlowWeight);

	std::vector<DecCache> decCaches;
	auto dec = decodeWithCache(targetTokens, mem, decCaches);
	float loss = 0.0f;
	std::vector<std::vector<float>> dDec(dec.size(), std::vector<float>((size_t)p.dModel, 0.0f));
	int bsz = (int)outProj_.b.size();
	int wOffset = bsz;
	for (size_t t = 0; t < dec.size(); t++) {
		auto logits = logitsAt(dec[t]);
		float maxv = -1e9f;
		for (float v : logits) maxv = std::max(maxv, v);
		double sum = 0.0;
		for (float v : logits) sum += std::exp((double)v - maxv);
		int tgt = (t < targetTokens.size()) ? targetTokens[t] : 0;
		double prob = std::exp((double)logits[tgt] - maxv) / std::max(1e-9, sum);
		loss += (float)-std::log(std::max(1e-9, prob));
		std::vector<float> probs((size_t)logits.size(), 0.0f);
		for (size_t i = 0; i < logits.size(); i++) {
			probs[i] = (float)(std::exp((double)logits[i] - maxv) / std::max(1e-9, sum));
		}
		float eps = std::max(0.0f, std::min(0.2f, p.labelSmoothing));
		float smoothNeg = (logits.size() > 1) ? (eps / (float)(logits.size() - 1)) : 0.0f;
		for (size_t i = 0; i < probs.size(); i++) {
			float targetProb = ((int)i == tgt) ? (1.0f - eps) : smoothNeg;
			float grad = probs[i] - targetProb;
			grad = clipGrad(grad, p.gradClip);
			outGradB_[i] += grad;
			for (int d = 0; d < outProj_.w.cols; d++) {
				int idx = wOffset + (int)i * outProj_.w.cols + d;
				float g = grad * dec[t][(size_t)d];
				g = clipGrad(g, p.gradClip);
				outGradW_[(size_t)((int)i * outProj_.w.cols + d)] += g;
				dDec[t][(size_t)d] += outProj_.w((int)i, d) * grad;
			}
		}
	}

	std::vector<std::vector<float>> dMem(mem.size(), std::vector<float>((size_t)p.dModel, 0.0f));
	for (int li = (int)decLayers_.size() - 1; li >= 0; li--) {
		auto &layer = decLayers_[(size_t)li];
		auto &cache = decCaches[(size_t)li];
		auto dLn3In = layerNormBackwardBatch(layer.ln3, cache.ln3, dDec, p);
		auto dZ = dLn3In;
		auto dFf = dLn3In;
		auto dZ2 = ffnBackward(dFf, layer.ffn, cache.ffn, p);
		for (size_t i = 0; i < dZ.size(); i++) for (size_t d = 0; d < dZ[i].size(); d++) dZ[i][d] += dZ2[i][d];
		auto dLn2In = layerNormBackwardBatch(layer.ln2, cache.ln2, dZ, p);
		auto dY = dLn2In;
		auto dCross = dLn2In;
		auto crossBack = attnBackward(layer.crossAttn, cache.cross, dCross, p);
		for (size_t i = 0; i < dY.size(); i++) for (size_t d = 0; d < dY[i].size(); d++) dY[i][d] += crossBack.first[i][d];
		for (size_t i = 0; i < dMem.size() && i < crossBack.second.size(); i++) {
			for (size_t d = 0; d < dMem[i].size(); d++) dMem[i][d] += crossBack.second[i][d];
		}
		auto dLn1In = layerNormBackwardBatch(layer.ln1, cache.ln1, dY, p);
		auto dX = dLn1In;
		auto dSelf = dLn1In;
		auto selfBack = attnBackward(layer.selfAttn, cache.self, dSelf, p);
		for (size_t i = 0; i < dX.size(); i++) for (size_t d = 0; d < dX[i].size(); d++) dX[i][d] += selfBack.first[i][d];
		dDec = dX;
	}
	applyEmbeddingGrad(targetTokens, dDec);

	auto selfFlowBackward = [&](const std::vector<std::vector<float>> &dOut, float weight) {
		if (dOut.empty() || weight <= 0.0f) return dOut;
		float a = 1.0f - 2.0f * weight / 3.0f;
		float b = weight / 3.0f;
		std::vector<std::vector<float>> dIn = dOut;
		for (size_t i = 0; i < dOut.size(); i++) {
			for (size_t d = 0; d < dOut[i].size(); d++) {
				float acc = a * dOut[i][d];
				if (i > 0) acc += b * dOut[i - 1][d];
				if (i + 1 < dOut.size()) acc += b * dOut[i + 1][d];
				dIn[i][d] = acc;
			}
		}
		return dIn;
	};
	dMem = selfFlowBackward(dMem, p.selfFlowWeight);

	std::vector<std::vector<float>> dMemText = dMem;
	std::vector<std::vector<float>> dMemGraph;
	if (!memGraph.empty()) {
		dMemText.assign(dMem.begin(), dMem.begin() + memText.size());
		dMemGraph.assign(dMem.begin() + memText.size(), dMem.end());
		if (fused) {
			auto backFuse = fuseMemoryBackward(fuseCache, dMemText, p.graphWeight);
			dMemText = backFuse.first;
			dMemGraph = backFuse.second;
		}
	}

	auto backpropEncoder = [&](std::vector<EncCache> &caches, std::vector<std::vector<float>> dEnc) {
		for (int li = (int)encLayers_.size() - 1; li >= 0; li--) {
			auto &layer = encLayers_[(size_t)li];
			auto &cache = caches[(size_t)li];
			auto dLn2In = layerNormBackwardBatch(layer.ln2, cache.ln2, dEnc, p);
			auto dY = dLn2In;
			auto dFf = dLn2In;
			auto dY2 = ffnBackward(dFf, layer.ffn, cache.ffn, p);
			for (size_t i = 0; i < dY.size(); i++) for (size_t d = 0; d < dY[i].size(); d++) dY[i][d] += dY2[i][d];
			auto dLn1In = layerNormBackwardBatch(layer.ln1, cache.ln1, dY, p);
			auto dX = dLn1In;
			auto dSelf = dLn1In;
			auto selfBack = attnBackward(layer.selfAttn, cache.attn, dSelf, p);
			for (size_t i = 0; i < dX.size(); i++) for (size_t d = 0; d < dX[i].size(); d++) dX[i][d] += selfBack.first[i][d];
			dEnc = dX;
		}
		return dEnc;
	};

	auto dEncText = backpropEncoder(encCacheText, dMemText);
	applyEmbeddingGrad(inputTokens, dEncText);
	if (p.bidirectionalWeight > 0.0f && !encCacheTextRev.empty()) {
		auto rev = reverseTokensPreserveSpecial(inputTokens);
		auto dEncTextRev = backpropEncoder(encCacheTextRev, dMemText);
		applyEmbeddingGrad(rev, dEncTextRev);
	}
	if (!memGraph.empty()) {
		auto dEncGraph = backpropEncoder(encCacheGraph, dMemGraph);
		applyEmbeddingGrad(graphTokens, dEncGraph);
		if (p.bidirectionalWeight > 0.0f && !encCacheGraphRev.empty()) {
			auto grev = reverseTokensPreserveSpecial(graphTokens);
			auto dEncGraphRev = backpropEncoder(encCacheGraphRev, dMemGraph);
			applyEmbeddingGrad(grev, dEncGraphRev);
		}
	}

	if (applyUpdate) {
		int accum = std::max(1, p.gradAccumSteps);
		embedStep_ += 1;
		for (size_t i = 0; i < tokGrad_.size(); i++) {
			float g = tokGrad_[i] / (float)accum;
			if (g != 0.0f) adamUpdate(tokEmbed_.data[i], g, tokMom1_[i], tokMom2_[i], embedStep_, p);
			tokGrad_[i] = 0.0f;
		}
		for (size_t i = 0; i < posGrad_.size(); i++) {
			float g = posGrad_[i] / (float)accum;
			if (g != 0.0f) adamUpdate(posEmbed_.data[i], g, posMom1_[i], posMom2_[i], embedStep_, p);
			posGrad_[i] = 0.0f;
		}

		outStep_ += 1;
		for (size_t i = 0; i < outProj_.b.size(); i++) {
			float g = outGradB_[i] / (float)accum;
			adamUpdate(outProj_.b[i], g, outMom1_[i], outMom2_[i], outStep_, p);
			outGradB_[i] = 0.0f;
		}
		for (int r = 0; r < outProj_.w.rows; r++) {
			for (int c = 0; c < outProj_.w.cols; c++) {
				size_t wIdx = (size_t)r * (size_t)outProj_.w.cols + (size_t)c;
				size_t momIdx = outProj_.b.size() + wIdx;
				float g = outGradW_[wIdx] / (float)accum;
				adamUpdate(outProj_.w(r, c), g, outMom1_[momIdx], outMom2_[momIdx], outStep_, p);
				outGradW_[wIdx] = 0.0f;
			}
		}

		for (auto &layer : encLayers_) {
			linearApplyAccum(layer.selfAttn.wq, p);
			linearApplyAccum(layer.selfAttn.wk, p);
			linearApplyAccum(layer.selfAttn.wv, p);
			linearApplyAccum(layer.selfAttn.wo, p);
			linearApplyAccum(layer.ffn.w1, p);
			linearApplyAccum(layer.ffn.w2, p);
			layerNormApplyAccum(layer.ln1, p);
			layerNormApplyAccum(layer.ln2, p);
		}
		for (auto &layer : decLayers_) {
			linearApplyAccum(layer.selfAttn.wq, p);
			linearApplyAccum(layer.selfAttn.wk, p);
			linearApplyAccum(layer.selfAttn.wv, p);
			linearApplyAccum(layer.selfAttn.wo, p);
			linearApplyAccum(layer.crossAttn.wq, p);
			linearApplyAccum(layer.crossAttn.wk, p);
			linearApplyAccum(layer.crossAttn.wv, p);
			linearApplyAccum(layer.crossAttn.wo, p);
			linearApplyAccum(layer.ffn.w1, p);
			linearApplyAccum(layer.ffn.w2, p);
			layerNormApplyAccum(layer.ln1, p);
			layerNormApplyAccum(layer.ln2, p);
			layerNormApplyAccum(layer.ln3, p);
		}
		linearApplyAccum(fuseAttn_.wq, p);
		linearApplyAccum(fuseAttn_.wk, p);
		linearApplyAccum(fuseAttn_.wv, p);
		linearApplyAccum(fuseAttn_.wo, p);
		linearApplyAccum(fuseGate_, p);
	}
	float lossOut = loss / std::max<size_t>(1, dec.size());
	float alpha = std::max(0.0f, std::min(1.0f, p.lossNoiseAlpha));
	if (alpha > 0.0f) {
		lossEma_ = (lossEma_ == 0.0f) ? lossOut : (alpha * lossOut + (1.0f - alpha) * lossEma_);
		lossOut = lossEma_;
	}
	return lossOut;
}

json TransformerModel::toJson() const {
	return json{{"vocabSize", params_.vocabSize}, {"dModel", params_.dModel}, {"nHeads", params_.nHeads},
				{"nLayers", params_.nLayers}, {"dFF", params_.dFF}, {"maxLen", params_.maxLen},
				{"maxTokens", params_.maxTokens}, {"temperature", params_.temperature},
				{"dynamicSampling", params_.dynamicSampling}, {"temperatureMin", params_.temperatureMin},
				{"temperatureMax", params_.temperatureMax}, {"topK", params_.topK},
				{"topP", params_.topP}, {"topPMin", params_.topPMin}, {"topPMax", params_.topPMax},
				{"dynamicWindow", params_.dynamicWindow}, {"dynamicRepetitionBoost", params_.dynamicRepetitionBoost},
				{"repetitionPenalty", params_.repetitionPenalty},
				{"repetitionSegmented", params_.repetitionSegmented},
				{"repetitionWindowRecent", params_.repetitionWindowRecent},
				{"repetitionWindowMid", params_.repetitionWindowMid},
				{"repetitionPenaltyRecent", params_.repetitionPenaltyRecent},
				{"repetitionPenaltyMid", params_.repetitionPenaltyMid},
				{"repetitionPenaltyOld", params_.repetitionPenaltyOld},
				{"noRepeatNgram", params_.noRepeatNgram}, {"minNewTokens", params_.minNewTokens},
				{"useLrSchedule", params_.useLrSchedule}, {"lrWarmupSteps", params_.lrWarmupSteps},
				{"lrCosineSteps", params_.lrCosineSteps}, {"lrMinRatio", params_.lrMinRatio},
				{"gradAccumSteps", params_.gradAccumSteps}, {"lossNoiseAlpha", params_.lossNoiseAlpha},
				{"attnChunkSize", params_.attnChunkSize},
				{"bidirectionalWeight", params_.bidirectionalWeight}, {"selfFlowWeight", params_.selfFlowWeight},
				{"tokenizerMode", params_.tokenizerMode}};
}

TransformerService::TransformerService() : model_(params_), tokenizer_(params_.vocabSize, params_.tokenizerMode) {}

static bool cacheGetTokens(std::unordered_map<std::string, std::pair<std::vector<int>, std::list<std::string>::iterator>> &cache,
							std::list<std::string> &order,
							const std::string &key,
							std::vector<int> &out) {
	auto it = cache.find(key);
	if (it == cache.end()) return false;
	order.splice(order.begin(), order, it->second.second);
	out = it->second.first;
	return true;
}

static void cachePutTokens(std::unordered_map<std::string, std::pair<std::vector<int>, std::list<std::string>::iterator>> &cache,
							std::list<std::string> &order,
							const std::string &key,
							const std::vector<int> &val,
							size_t maxEntries) {
	auto it = cache.find(key);
	if (it != cache.end()) {
		it->second.first = val;
		order.splice(order.begin(), order, it->second.second);
		return;
	}
	order.push_front(key);
	cache.emplace(key, std::make_pair(val, order.begin()));
	while (cache.size() > maxEntries) {
		auto last = order.back();
		order.pop_back();
		cache.erase(last);
	}
}

json TransformerService::chat(const std::string &text, const std::string &graphContext, int maxTokens) {
	std::vector<std::vector<float>> empty;
	return chat(text, graphContext, empty, maxTokens);
}

json TransformerService::chat(const std::string &text, const std::string &graphContext,
					  const std::vector<std::vector<float>> &graphEmbeddings, int maxTokens) {
	std::string graphNorm = normalizeGraphText(graphContext);
	std::string key = hashKey(text, graphNorm, maxTokens, params_);
	{
		std::lock_guard<std::mutex> lock(cacheMu_);
		auto it = replyCache_.find(key);
		if (it != replyCache_.end()) {
			int64_t age = nowMs() - it->second.second.first;
			if (age <= params_.cacheTtlMs) {
				replyOrder_.splice(replyOrder_.begin(), replyOrder_, it->second.second.second);
				return it->second.first;
			}
		}
	}
	int maxLen = params_.maxLen;
	std::vector<int> input;
	std::vector<int> graph;
	{
		std::lock_guard<std::mutex> lock(cacheMu_);
		if (!cacheGetTokens(tokenCache_, tokenOrder_, "t:" + text, input)) {
			input = tokenizer_.encode(text, maxLen, true);
			cachePutTokens(tokenCache_, tokenOrder_, "t:" + text, input, (size_t)std::max(32, params_.cacheTokenEntries));
		}
		if (!cacheGetTokens(tokenCache_, tokenOrder_, "g:" + graphNorm, graph)) {
			graph = tokenizer_.encode(graphNorm, maxLen, true);
			cachePutTokens(tokenCache_, tokenOrder_, "g:" + graphNorm, graph, (size_t)std::max(32, params_.cacheTokenEntries));
		}
	}
	auto out = model_.generate(input, graph, graphEmbeddings, std::max(8, maxTokens), params_.graphWeight);
	std::string reply = tokenizer_.decode(out);
	json result{{"reply", reply}, {"tokens", out}};
	if (params_.enableDiagnostics) {
		auto memText = model_.encode(input);
		std::vector<std::vector<float>> memGraph;
		if (!graphEmbeddings.empty()) {
			memGraph = graphEmbeddings;
		} else if (!graph.empty()) {
			memGraph = model_.encode(graph);
		}
		float memTextNorm = avgNorm(memText);
		float memGraphNorm = avgNorm(memGraph);
		float memMixNorm = avgNorm(model_.fuseMemory(memText, memGraph, params_.graphWeight));
		result["diagnostics"] = json{
			{"inputTokens", (int)input.size()},
			{"graphTokens", (int)graph.size()},
			{"outputTokens", (int)out.size()},
			{"memTextNorm", memTextNorm},
			{"memGraphNorm", memGraphNorm},
			{"memMixNorm", memMixNorm},
			{"graphWeight", params_.graphWeight}
		};
	}
	{
		std::lock_guard<std::mutex> lock(cacheMu_);
		auto it = replyCache_.find(key);
		if (it != replyCache_.end()) replyCache_.erase(it);
		replyOrder_.push_front(key);
		replyCache_[key] = {result, {nowMs(), replyOrder_.begin()}};
		while ((int)replyCache_.size() > std::max(8, params_.cacheMaxEntries)) {
			auto last = replyOrder_.back();
			replyOrder_.pop_back();
			replyCache_.erase(last);
		}
	}
	return result;
}

float TransformerService::jaccard(const std::vector<int> &a, const std::vector<int> &b) const {
	std::unordered_set<int> A(a.begin(), a.end());
	std::unordered_set<int> B(b.begin(), b.end());
	if (A.empty() && B.empty()) return 1.0f;
	int inter = 0;
	for (int v : A) if (B.count(v)) inter++;
	int uni = (int)A.size() + (int)B.size() - inter;
	return uni <= 0 ? 0.0f : (float)inter / (float)uni;
}

float TransformerService::computeContrastiveLoss(const std::vector<int> &a, const std::vector<int> &b) const {
	if (a.empty() || b.empty()) return 1.0f;
	float j = jaccard(a, b);
	float loss = 1.0f - j;
	return std::max(0.0f, std::min(1.0f, loss));
}

float TransformerService::sampleLoss(const TrainSample &s) {
	auto input = tokenizer_.encode(s.input, params_.maxLen, true);
	auto graph = tokenizer_.encode(normalizeGraphText(s.graph), params_.maxLen, true);
	auto target = tokenizer_.encode(s.target, params_.maxLen, true);
	return model_.trainOnSample(input, graph, target, params_.lr);
}

json TransformerService::pretrain(const std::vector<TrainSample> &samples, int epochs, float lr) {
	params_.lr = lr;
	float loss = 0.0f;
	for (int e = 0; e < epochs; e++) {
		float epochLoss = 0.0f;
		for (auto &s : samples) epochLoss += sampleLoss(s);
		loss = epochLoss / std::max<size_t>(1, samples.size());
	}
	return json{{"ok", true}, {"loss", loss}};
}

json TransformerService::jointTrain(const std::vector<TrainSample> &samples, int epochs, float lr, float graphWeight) {
	params_.lr = lr;
	params_.graphWeight = graphWeight;
	float loss = 0.0f;
	float bestLoss = 1e9f;
	int noImprove = 0;
	const int patience = 3;
	const float minDelta = 1e-4f;
	for (int e = 0; e < epochs; e++) {
		float epochLoss = 0.0f;
		std::vector<size_t> order(samples.size());
		for (size_t i = 0; i < order.size(); i++) order[i] = i;
		std::shuffle(order.begin(), order.end(), rng_);
		for (size_t idx = 0; idx < order.size(); idx++) {
			auto &s = samples[order[idx]];
			float base = sampleLoss(s);
			auto input = tokenizer_.encode(s.input, params_.maxLen, true);
			auto graph = tokenizer_.encode(normalizeGraphText(s.graph), params_.maxLen, true);
			auto target = tokenizer_.encode(s.target, params_.maxLen, true);
			float seqLoss = model_.trainOnSample(input, {}, target, params_.lr * 0.6f);
			float graphLoss = model_.trainOnSample({}, graph, target, params_.lr * 0.6f);
			float reconLoss = model_.trainOnSample(input, {}, graph, params_.lr * 0.4f);
			float align = 1.0f - jaccard(input, graph);

			// Add contrastive loss to align text and graph embeddings
			float contrastiveLoss = computeContrastiveLoss(input, graph);

			epochLoss += base + 0.5f * seqLoss + graphWeight * (graphLoss + reconLoss + align + contrastiveLoss);
		}
		loss = epochLoss / std::max<size_t>(1, samples.size());
		if (loss + minDelta < bestLoss) {
			bestLoss = loss;
			noImprove = 0;
		} else if (++noImprove >= patience) {
			break;
		}
	}
	return json{{"ok", true}, {"loss", loss}, {"graphWeight", graphWeight}};
}

json TransformerService::optimizeGA(const std::vector<TrainSample> &samples, int generations, int population) {
	struct Cand { TransformerParams p; float score; };
	std::vector<Cand> pop;
	pop.reserve((size_t)population);
	std::uniform_int_distribution<int> dModel(64, 256);
	std::uniform_int_distribution<int> heads(2, 8);
	std::uniform_int_distribution<int> layers(1, 6);
	std::uniform_int_distribution<int> dFF(128, 512);
	for (int i = 0; i < population; i++) {
		TransformerParams p = params_;
		p.dModel = dModel(rng_);
		p.nHeads = heads(rng_);
		p.nLayers = layers(rng_);
		p.dFF = dFF(rng_);
		pop.push_back({p, 0.0f});
	}
	auto eval = [&](TransformerParams &p) {
		TransformerModel tmp(p);
		Tokenizer tok(p.vocabSize, p.tokenizerMode);
		float loss = 0.0f;
		for (auto &s : samples) {
			auto input = tok.encode(s.input, p.maxLen, true);
			auto graph = tok.encode(s.graph, p.maxLen, true);
			auto target = tok.encode(s.target, p.maxLen, true);
			loss += tmp.trainOnSample(input, graph, target, p.lr);
		}
		return -loss / std::max<size_t>(1, samples.size());
	};
	for (int g = 0; g < generations; g++) {
		for (auto &c : pop) c.score = eval(c.p);
		std::sort(pop.begin(), pop.end(), [](auto &a, auto &b) { return a.score > b.score; });
		int elite = std::max(2, population / 4);
		for (int i = elite; i < population; i++) {
			auto &pa = pop[i % elite].p;
			auto &pb = pop[(i + 1) % elite].p;
			TransformerParams child = pa;
			if (rng_() % 2) child.dModel = pb.dModel;
			if (rng_() % 2) child.nHeads = pb.nHeads;
			if (rng_() % 2) child.nLayers = pb.nLayers;
			if (rng_() % 2) child.dFF = pb.dFF;
			if (rng_() % 3 == 0) child.dModel = std::max(32, child.dModel + (int)(rng_() % 32) - 16);
			pop[i].p = child;
		}
	}
	params_ = pop.front().p;
	model_ = TransformerModel(params_);
	return json{{"ok", true}, {"best", model_.toJson()}};
}

json TransformerService::verify(const std::string &text, const std::string &graphContext, const std::string &reply) const {
	auto input = tokenizer_.encode(text, params_.maxLen, true);
	auto graph = tokenizer_.encode(normalizeGraphText(graphContext), params_.maxLen, true);
	auto rep = tokenizer_.encode(reply, params_.maxLen, true);
	float sim = jaccard(input, rep) * 0.6f + jaccard(graph, rep) * 0.4f;
	bool ok = sim >= params_.verifyThreshold;
	return json{{"ok", ok}, {"score", sim}};
}

json TransformerService::params() const {
	auto j = model_.toJson();
	j["lr"] = params_.lr;
	j["useLrSchedule"] = params_.useLrSchedule;
	j["lrWarmupSteps"] = params_.lrWarmupSteps;
	j["lrCosineSteps"] = params_.lrCosineSteps;
	j["lrMinRatio"] = params_.lrMinRatio;
	j["gradAccumSteps"] = params_.gradAccumSteps;
	j["lossNoiseAlpha"] = params_.lossNoiseAlpha;
	j["attnChunkSize"] = params_.attnChunkSize;
	j["graphWeight"] = params_.graphWeight;
	j["verifyThreshold"] = params_.verifyThreshold;
	j["labelSmoothing"] = params_.labelSmoothing;
	j["gradClip"] = params_.gradClip;
	j["adamBeta1"] = params_.adamBeta1;
	j["adamBeta2"] = params_.adamBeta2;
	j["adamEps"] = params_.adamEps;
	j["weightDecay"] = params_.weightDecay;
	j["repetitionPenalty"] = params_.repetitionPenalty;
	j["noRepeatNgram"] = params_.noRepeatNgram;
	j["minNewTokens"] = params_.minNewTokens;
	j["enableDiagnostics"] = params_.enableDiagnostics;
	j["bidirectionalWeight"] = params_.bidirectionalWeight;
	j["selfFlowWeight"] = params_.selfFlowWeight;
	j["cacheMaxEntries"] = params_.cacheMaxEntries;
	j["cacheTokenEntries"] = params_.cacheTokenEntries;
	j["cacheTtlMs"] = params_.cacheTtlMs;
	return j;
}

void TransformerService::applyParams(const json &patch) {
	TransformerParams p = params_;
	if (patch.contains("vocabSize")) p.vocabSize = patch.value("vocabSize", p.vocabSize);
	if (patch.contains("dModel")) p.dModel = patch.value("dModel", p.dModel);
	if (patch.contains("nHeads")) p.nHeads = patch.value("nHeads", p.nHeads);
	if (patch.contains("nLayers")) p.nLayers = patch.value("nLayers", p.nLayers);
	if (patch.contains("dFF")) p.dFF = patch.value("dFF", p.dFF);
	if (patch.contains("maxLen")) p.maxLen = patch.value("maxLen", p.maxLen);
	if (patch.contains("maxTokens")) p.maxTokens = patch.value("maxTokens", p.maxTokens);
	if (patch.contains("lr")) p.lr = patch.value("lr", p.lr);
	if (patch.contains("useLrSchedule")) p.useLrSchedule = patch.value("useLrSchedule", p.useLrSchedule);
	if (patch.contains("lrWarmupSteps")) p.lrWarmupSteps = patch.value("lrWarmupSteps", p.lrWarmupSteps);
	if (patch.contains("lrCosineSteps")) p.lrCosineSteps = patch.value("lrCosineSteps", p.lrCosineSteps);
	if (patch.contains("lrMinRatio")) p.lrMinRatio = patch.value("lrMinRatio", p.lrMinRatio);
	if (patch.contains("gradAccumSteps")) p.gradAccumSteps = patch.value("gradAccumSteps", p.gradAccumSteps);
	if (patch.contains("lossNoiseAlpha")) p.lossNoiseAlpha = patch.value("lossNoiseAlpha", p.lossNoiseAlpha);
	if (patch.contains("attnChunkSize")) p.attnChunkSize = patch.value("attnChunkSize", p.attnChunkSize);
	if (patch.contains("graphWeight")) p.graphWeight = patch.value("graphWeight", p.graphWeight);
	if (patch.contains("verifyThreshold")) p.verifyThreshold = patch.value("verifyThreshold", p.verifyThreshold);
	if (patch.contains("temperature")) p.temperature = patch.value("temperature", p.temperature);
	if (patch.contains("dynamicSampling")) p.dynamicSampling = patch.value("dynamicSampling", p.dynamicSampling);
	if (patch.contains("temperatureMin")) p.temperatureMin = patch.value("temperatureMin", p.temperatureMin);
	if (patch.contains("temperatureMax")) p.temperatureMax = patch.value("temperatureMax", p.temperatureMax);
	if (patch.contains("topK")) p.topK = patch.value("topK", p.topK);
	if (patch.contains("topP")) p.topP = patch.value("topP", p.topP);
	if (patch.contains("topPMin")) p.topPMin = patch.value("topPMin", p.topPMin);
	if (patch.contains("topPMax")) p.topPMax = patch.value("topPMax", p.topPMax);
	if (patch.contains("dynamicWindow")) p.dynamicWindow = patch.value("dynamicWindow", p.dynamicWindow);
	if (patch.contains("dynamicRepetitionBoost")) p.dynamicRepetitionBoost = patch.value("dynamicRepetitionBoost", p.dynamicRepetitionBoost);
	if (patch.contains("labelSmoothing")) p.labelSmoothing = patch.value("labelSmoothing", p.labelSmoothing);
	if (patch.contains("gradClip")) p.gradClip = patch.value("gradClip", p.gradClip);
	if (patch.contains("adamBeta1")) p.adamBeta1 = patch.value("adamBeta1", p.adamBeta1);
	if (patch.contains("adamBeta2")) p.adamBeta2 = patch.value("adamBeta2", p.adamBeta2);
	if (patch.contains("adamEps")) p.adamEps = patch.value("adamEps", p.adamEps);
	if (patch.contains("weightDecay")) p.weightDecay = patch.value("weightDecay", p.weightDecay);
	if (patch.contains("repetitionPenalty")) p.repetitionPenalty = patch.value("repetitionPenalty", p.repetitionPenalty);
	if (patch.contains("repetitionSegmented")) p.repetitionSegmented = patch.value("repetitionSegmented", p.repetitionSegmented);
	if (patch.contains("repetitionWindowRecent")) p.repetitionWindowRecent = patch.value("repetitionWindowRecent", p.repetitionWindowRecent);
	if (patch.contains("repetitionWindowMid")) p.repetitionWindowMid = patch.value("repetitionWindowMid", p.repetitionWindowMid);
	if (patch.contains("repetitionPenaltyRecent")) p.repetitionPenaltyRecent = patch.value("repetitionPenaltyRecent", p.repetitionPenaltyRecent);
	if (patch.contains("repetitionPenaltyMid")) p.repetitionPenaltyMid = patch.value("repetitionPenaltyMid", p.repetitionPenaltyMid);
	if (patch.contains("repetitionPenaltyOld")) p.repetitionPenaltyOld = patch.value("repetitionPenaltyOld", p.repetitionPenaltyOld);
	if (patch.contains("noRepeatNgram")) p.noRepeatNgram = patch.value("noRepeatNgram", p.noRepeatNgram);
	if (patch.contains("minNewTokens")) p.minNewTokens = patch.value("minNewTokens", p.minNewTokens);
	if (patch.contains("enableDiagnostics")) p.enableDiagnostics = patch.value("enableDiagnostics", p.enableDiagnostics);
	if (patch.contains("bidirectionalWeight")) p.bidirectionalWeight = patch.value("bidirectionalWeight", p.bidirectionalWeight);
	if (patch.contains("selfFlowWeight")) p.selfFlowWeight = patch.value("selfFlowWeight", p.selfFlowWeight);
	if (patch.contains("cacheMaxEntries")) p.cacheMaxEntries = patch.value("cacheMaxEntries", p.cacheMaxEntries);
	if (patch.contains("cacheTokenEntries")) p.cacheTokenEntries = patch.value("cacheTokenEntries", p.cacheTokenEntries);
	if (patch.contains("cacheTtlMs")) p.cacheTtlMs = patch.value("cacheTtlMs", p.cacheTtlMs);
	if (patch.contains("tokenizerMode")) p.tokenizerMode = patch.value("tokenizerMode", p.tokenizerMode);
	bool rebuild = p.vocabSize != params_.vocabSize || p.dModel != params_.dModel || p.nHeads != params_.nHeads ||
				   p.nLayers != params_.nLayers || p.dFF != params_.dFF || p.maxLen != params_.maxLen ||
				   p.tokenizerMode != params_.tokenizerMode;
	params_ = p;
	if (rebuild) {
		model_ = TransformerModel(params_);
		tokenizer_ = Tokenizer(params_.vocabSize, params_.tokenizerMode);
	} else {
		model_.updateParams(params_);
		tokenizer_.setMode(params_.tokenizerMode);
	}
	{
		std::lock_guard<std::mutex> lock(cacheMu_);
		replyCache_.clear();
		replyOrder_.clear();
		tokenCache_.clear();
		tokenOrder_.clear();
	}
}

} // namespace transformer
