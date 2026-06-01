# Design: Handling Sensitive Data in ML Pipelines

**Difficulty:** Medium
**Time:** ~20–30 minutes
**Focus areas:** Data privacy, anonymization, synthetic data, model security, differential privacy

---

## Problem Statement

Your team needs to build ML models using users' **trading history** — highly sensitive personal financial data. The pipeline has two phases:

1. **Development:** Engineers and data scientists prototype in an **insecure development environment** not approved for client data.
2. **Production:** Models are trained and deployed on real data in a secure environment.

**Questions:**
1. During development, how do you provide data scientists with usable data without compromising user trading history?
2. Post-release, what are the security concerns around the deployed model itself?

---

## Part 1: Development Environment — Protecting User Data

### Option A: Data Anonymization

Remove or obscure Personally Identifiable Information (PII) while preserving statistical structure.

**Techniques:**
- **Pseudonymization:** Replace `user_id` with a hash (`SHA-256(user_id + secret_salt)`). Reversible with the salt (not truly anonymous).
- **Generalization:** Replace exact values with ranges (e.g., `trade_value = $12,350` → `$10K–$15K`).
- **Suppression:** Remove rare or identifying records entirely.
- **Data masking:** Replace sensitive fields with realistic-looking but fake values.

**Trade-offs:**
- Preserves real statistical distributions.
- Risk: re-identification attacks if enough auxiliary information exists (e.g., a user's known trade on a specific date can de-anonymize them).
- Suitable when the statistical properties must match production closely.

---

### Option B: Synthetic Data Generation

Generate entirely artificial data that has the same statistical properties as real data but contains no real user records.

**Techniques:**
- **Statistical sampling:** Sample from the distribution of real data (mean, variance, correlations).
- **GANs (Generative Adversarial Networks):** Train a GAN on real data to generate synthetic records that are indistinguishable statistically.
- **Agent-based simulation:** Simulate users following trading patterns derived from aggregate statistics.

**Trade-offs:**
- No real user data leaves the secure environment.
- Developers cannot inadvertently leak real data via logs, error messages, or bug reports.
- Synthetic data may not capture all edge cases and rare events.
- Suitable when compliance requirements prohibit any real data in dev.

---

### Option C: Differential Privacy

Add carefully calibrated noise to the data or query results such that no individual's record can be inferred from the output.

- Can share aggregate statistics (e.g., "average trade frequency per sector") with mathematically proven privacy guarantees (epsilon-delta DP).
- Not suitable for providing row-level records to dev teams.
- More applicable to releasing model outputs or analytics.

---

### Recommended Approach for Development

1. **Production environment only:** Store raw trading data. No raw data leaves.
2. **Data pipeline:** Generate a **synthetic or anonymized dataset** in the secure environment → export only that to dev.
3. **Realistic but fake:** Use GAN-generated or statistically sampled data that preserves distributions (sector mix, trade frequency, volume ranges).
4. **Sacrifice:** The dev dataset may miss rare events and exact relationships. Models trained in dev need to be re-validated against real data in production before release.

---

### Preventing Accidental Leaks

Even with anonymized/synthetic data, engineers can inadvertently expose data:
- **Logs:** Ensure data engineers know not to log raw field values; use structured logging with field-level redaction.
- **Model training output:** Check that training logs don't print raw rows.
- **Jupyter notebooks:** Notebooks with real data output must stay in the secure environment.
- **Encryption at rest and in transit:** Even dev data should be encrypted.

---

## Part 2: Post-Release Security Concerns

### Model Inversion / Extraction Attacks

A trained ML model can "memorize" training data, especially with small datasets.

**Model inversion:** An attacker queries the model repeatedly with crafted inputs to infer training data. Example: if the model is a classifier that outputs probabilities, an attacker can reconstruct individual records.

**Membership inference:** An attacker can determine whether a specific record was part of the training set (e.g., "Was this specific trade made by user X?").

**Mitigations:**
- **Differential privacy during training:** Add noise to gradients (DP-SGD). Provides mathematical guarantees that individual records cannot be extracted.
- **Rate limiting / access controls:** Limit how many queries a single user/client can make to the model API.
- **Model output truncation:** Return only the top prediction, not full probability distributions.
- **Federated learning:** Train on decentralized data — model never sees raw data, only gradient updates.

---

### Model Outputs Containing Sensitive Information

If a model is trained on trading history and then deployed for trading recommendations, its outputs could inadvertently expose:
- Which stocks a client's firm trades heavily (inferred from recommendation patterns).
- Timing patterns that reveal internal trading strategies.

**Mitigations:**
- Audit model outputs for information leakage during evaluation.
- Post-process outputs to add noise or round to buckets.

---

### Access Controls on Model Artifacts

- The model file itself contains information derived from training data.
- Store model artifacts with the same access controls as the training data.
- Audit who downloads or deploys model versions.

---

## Summary Table

| Concern | Risk | Mitigation |
|---|---|---|
| Dev access to raw data | Data breach / leakage | Synthetic or anonymized data only |
| Logs in dev | Accidental PII in logs | Structured logging + field redaction |
| Model memorization | Training data extraction | DP-SGD, rate limiting |
| Membership inference | User re-identification | DP, output truncation |
| Model artifact access | Model = compressed training data | Same access controls as raw data |
| Jupyter notebooks | Exported data in notebooks | Keep notebooks in secure zone |

---

## What to Cover in Your Answer

- [ ] Why raw data cannot go to the dev environment
- [ ] Anonymization: techniques and limitations (re-identification risk)
- [ ] Synthetic data: how it's generated, what's preserved, what's lost
- [ ] Differential privacy: concept (add noise to protect individuals) — when applicable
- [ ] Concrete sacrifices made (edge cases missed, exact distributions not preserved)
- [ ] Post-release: model inversion and membership inference attacks
- [ ] Mitigations: DP training, rate limiting, access controls

---

## Follow-Up Questions

1. "You want to share aggregate statistics (e.g., average trade frequency by sector) from production with dev. Is this safe?"
*(Answer: Use differential privacy — add calibrated noise to aggregates so individuals can't be inferred.)*

2. "A data scientist accidentally ran a query in dev that printed 10 real user IDs to stdout. What is your incident response?"

3. "Your model achieves 99% accuracy on your test set. A security researcher claims they can reconstruct 30% of your training records. What's happening and how do you fix it?"
*(Answer: Model is overfitting / memorizing. Apply DP-SGD during training; evaluate with membership inference tests.)*

4. "What's the difference between anonymization and pseudonymization? Which one provides stronger privacy guarantees?"
