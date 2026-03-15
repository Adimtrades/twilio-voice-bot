"use strict";

const DEBUG_BEHAVIOUR = process.env.DEBUG_BEHAVIOUR === "true";

function debugLog(event, payload = {}) {
  if (!DEBUG_BEHAVIOUR) return;
  console.log(`[behaviour] ${event}`, payload);
}

function createConversationState() {
  return {
    urgency_score: 0,
    trust_score: 5,
    resistance_type: null,
    commitment_stage: "info",
    off_topic_counter: 0,
    last_customer_problem_summary: "",
    preferred_time_hint: "",
    objection_history: [],
    loss_framing_used: false,
    recent_problem_mentions: []
  };
}

function clampScore(value) {
  return Math.max(0, Math.min(10, Number(value || 0)));
}

function detectResistanceType(text = "") {
  const t = String(text || "").toLowerCase();
  if (!t) return null;

  if (/too expensive|just checking|not sure yet|can't afford|costs? too much/.test(t)) return "price";
  if (/busy|maybe later|not today|another time|call back later/.test(t)) return "time";
  if (/last tradie was bad|dont want to get ripped|don't want to get ripped|can i trust|not convinced/.test(t)) return "trust";
  if (/i need to think|not ready to lock|dont want to commit|don't want to commit|maybe/.test(t)) return "commitment";
  return null;
}

function detectPreferredTimeHint(text = "") {
  const t = String(text || "").toLowerCase();
  if (/morning|before noon|am\b/.test(t)) return "morning";
  if (/afternoon|after lunch|pm\b/.test(t)) return "afternoon";
  if (/today/.test(t)) return "today";
  if (/tomorrow/.test(t)) return "tomorrow";
  return "";
}

function updateUrgencyScore(state, text = "") {
  const t = String(text || "").toLowerCase();
  if (!t) return state;

  const urgencySignals = ["flood", "urgent", "today", "leak getting worse", "asap", "emergency", "getting worse"];
  const stressSignals = ["stressed", "panicking", "really worried", "frustrating", "desperate"];

  if (urgencySignals.some((w) => t.includes(w))) state.urgency_score += 2;
  if (stressSignals.some((w) => t.includes(w))) state.urgency_score += 1;

  const mentionKey = t.replace(/\s+/g, " ").trim().slice(0, 64);
  if (mentionKey) {
    state.recent_problem_mentions = [...(state.recent_problem_mentions || []).slice(-3), mentionKey];
    const duplicateCount = state.recent_problem_mentions.filter((m) => m === mentionKey).length;
    if (duplicateCount >= 2) state.urgency_score += 1;
  }

  state.urgency_score = clampScore(state.urgency_score);
  return state;
}

function updateCommitmentStage(state, step = "") {
  if (["intent", "job", "address"].includes(step)) state.commitment_stage = "info";
  else if (["name", "access", "time", "pickSlot"].includes(step)) state.commitment_stage = "interest";
  else if (step === "confirm") state.commitment_stage = "decision";
  else if (step === "booked") state.commitment_stage = "booking";
  return state;
}

function updateConversationState(state, text = "", sessionStep = "") {
  const next = state || createConversationState();
  updateUrgencyScore(next, text);
  updateCommitmentStage(next, sessionStep);

  const resistance = detectResistanceType(text);
  if (resistance) {
    next.resistance_type = resistance;
    next.objection_history = [...(next.objection_history || []).slice(-5), resistance];
  }

  const timeHint = detectPreferredTimeHint(text);
  if (timeHint) next.preferred_time_hint = timeHint;

  if (/leak|flood|blocked|no power|broken|issue|problem/.test(String(text || "").toLowerCase())) {
    next.last_customer_problem_summary = String(text || "").trim().slice(0, 180);
  }

  if (next.resistance_type === "trust") next.trust_score = clampScore(next.trust_score - 1);
  else if (next.commitment_stage === "decision") next.trust_score = clampScore(next.trust_score + 1);

  debugLog("state_update", {
    urgency_score: next.urgency_score,
    trust_score: next.trust_score,
    resistance_type: next.resistance_type,
    commitment_stage: next.commitment_stage,
    preferred_time_hint: next.preferred_time_hint
  });

  return next;
}

function generateGuidedChoice(questionType = "time") {
  if (questionType === "day") return "Is today or tomorrow easier for you?";
  if (questionType === "time") return "Would morning or afternoon suit you better?";
  if (questionType === "confirmation") return "Would you like to lock this in now, or would a quick callback be easier?";
  return "Would morning or afternoon suit you better?";
}

function applyLossFraming(problemDescription = "this issue") {
  const problem = String(problemDescription || "this issue").trim();
  return `If ${problem} continues, it can become more expensive or cause further damage.`;
}

function redirectOffTopic(state, fallbackQuestion = "Is the issue still happening now?") {
  const next = state || createConversationState();
  next.off_topic_counter = Number(next.off_topic_counter || 0) + 1;
  return `That sounds frustrating. Let’s make sure we get this sorted — ${fallbackQuestion}`;
}

function addTrustSignal(stage = "early") {
  if (stage === "decision") return "The technician will first identify the source, then explain the fix clearly.";
  return "We handle this type of issue often.";
}

function memoryRecallResponse(state, availabilityText = "") {
  const hint = state?.preferred_time_hint;
  if (!hint) return "";
  if (!availabilityText) return `You mentioned ${hint}s work best, so I’ll prioritise that.`;
  return `You mentioned ${hint}s work best — ${availabilityText}`;
}

function objectionResponse(resistanceType = null) {
  if (resistanceType === "price") return "That makes sense. The visit is focused on finding the cause first so you can choose the fix with clarity. Would morning or afternoon suit you better for an assessment?";
  if (resistanceType === "time") return "Totally fair. We can keep this quick and work around your day. Is today or tomorrow easier?";
  if (resistanceType === "trust") return "I hear you. We keep it transparent — clear diagnosis first, then options before any work starts. Would a short visit today help?";
  if (resistanceType === "commitment") return "No pressure. We can lock a provisional time and you can still adjust if needed. Would morning or afternoon be easier?";
  return "No problem — let’s take this one step at a time.";
}

function reflectAndGuide(problemSummary = "the issue", nextStep = "let’s organise the next step") {
  const clean = String(problemSummary || "the issue").trim();
  return `So ${clean} — does that sound right? ${nextStep}`;
}

function tonePolish(responseText = "", { shorten = false } = {}) {
  let text = String(responseText || "").replace(/\s+/g, " ").trim();
  text = text.replace(/!{2,}/g, "!");
  text = text.replace(/\b(act now|last chance|don’t miss out|dont miss out)\b/gi, "");
  text = text.replace(/\s{2,}/g, " ").trim();

  if (shorten && text.length > 190) {
    const sentences = text.split(/(?<=[.!?])\s+/).slice(0, 2).join(" ");
    text = sentences || text.slice(0, 190);
  }

  if (!text) return "No worries — let’s keep going.";
  return text;
}

module.exports = {
  createConversationState,
  updateConversationState,
  generateGuidedChoice,
  applyLossFraming,
  redirectOffTopic,
  addTrustSignal,
  memoryRecallResponse,
  objectionResponse,
  reflectAndGuide,
  tonePolish
};
