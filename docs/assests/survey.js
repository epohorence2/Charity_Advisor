import { apiFetch, renderCharityCard, setLiveMessage } from './app.js';

const form = document.getElementById('survey-form');
const resultsContainer = document.getElementById('results');
const errorEl = document.getElementById('survey-error');
const loadMoreBtn = document.getElementById('load-more');
const progressEl = document.getElementById('progress');
const progressCountEl = progressEl?.querySelector('[data-count]');
const resultsHeading = document.getElementById('results-heading');

const REQUIRED_FIELDS = ['q_issue_family', 'q_impact_mode', 'q_geography'];
const state = {
  answersPayload: [],
  cursor: null,
};

if (form) {
  form.addEventListener('input', updateProgress);
  form.addEventListener('change', updateProgress);
  form.addEventListener('submit', handleSubmit);
}

if (loadMoreBtn) {
  loadMoreBtn.addEventListener('click', () => fetchRecommendations({ append: true }));
}

updateProgress();

function handleSubmit(event) {
  event.preventDefault();
  const answers = serializeForm(form);
  state.answersPayload = answers;
  state.cursor = null;
  fetchRecommendations({ append: false });
}

function serializeForm(formElement) {
  const formData = new FormData(formElement);
  const answers = [];
  const seen = new Set();

  for (const [name, value] of formData.entries()) {
    if (name === 'q_topics') continue;
    if (!value) continue;
    if (seen.has(name)) continue;
    seen.add(name);
    answers.push({ question_id: name, value });
  }

  const topics = formData.getAll('q_topics').filter(Boolean);
  if (topics.length) {
    answers.push({ question_id: 'q_topics', value: topics });
  }

  return answers;
}

async function fetchRecommendations({ append }) {
  if (!state.answersPayload.length) {
    setLiveMessage(errorEl, 'Please answer the required questions before submitting.');
    return;
  }

  toggleSubmitLoading(!append);
  toggleLoadMoreLoading(append, true);
  setLiveMessage(errorEl, '');

  try {
    const payload = {
      answers: state.answersPayload,
      cursor: append ? state.cursor : null,
      limit: 3,
    };

    const data = await apiFetch('/api/recommend', {
      method: 'POST',
      body: JSON.stringify(payload),
    });

    if (!append) {
      resultsContainer.textContent = '';
    }

    const charities = data.charities ?? [];
    if (!charities.length && !append) {
      const empty = document.createElement('p');
      empty.className = 'muted';
      empty.textContent = 'No matches yet. Try widening your preferences.';
      resultsContainer.appendChild(empty);
    } else {
      charities.forEach((charity) => resultsContainer.appendChild(renderCharityCard(charity)));
    }

    state.cursor = data.cursor || null;
    updateLoadMoreButton();
    focusResults(charities.length, append);
  } catch (error) {
    console.error(error);
    setLiveMessage(errorEl, error.message || 'Unable to fetch recommendations.');
  } finally {
    toggleSubmitLoading(false);
    toggleLoadMoreLoading(append, false);
  }
}

function toggleSubmitLoading(isLoading) {
  if (!form) return;
  const submitBtn = form.querySelector('button[type="submit"]');
  if (!submitBtn) return;
  if (isLoading) {
    if (!submitBtn.dataset.originalLabel) {
      submitBtn.dataset.originalLabel = submitBtn.textContent;
    }
    submitBtn.textContent = submitBtn.dataset.loadingLabel || 'Submitting…';
  } else if (submitBtn.dataset.originalLabel) {
    submitBtn.textContent = submitBtn.dataset.originalLabel;
  }
  submitBtn.disabled = isLoading;
  form.setAttribute('aria-busy', String(isLoading));
}

function toggleLoadMoreLoading(shouldAffect, isLoading) {
  if (!shouldAffect || !loadMoreBtn) return;
  if (isLoading) {
    if (!loadMoreBtn.dataset.originalLabel) {
      loadMoreBtn.dataset.originalLabel = loadMoreBtn.textContent;
    }
    loadMoreBtn.textContent = 'Loading…';
  } else if (loadMoreBtn.dataset.originalLabel) {
    loadMoreBtn.textContent = loadMoreBtn.dataset.originalLabel;
  }
  loadMoreBtn.disabled = isLoading;
}

function updateLoadMoreButton() {
  if (!loadMoreBtn) return;
  if (state.cursor) {
    loadMoreBtn.hidden = false;
    loadMoreBtn.disabled = false;
  } else {
    loadMoreBtn.hidden = true;
  }
}

function focusResults(newCount, append) {
  if (!resultsHeading) return;
  const existingCards = resultsContainer.querySelectorAll('.card').length;
  const total = append ? existingCards : newCount;
  resultsHeading.textContent = total
    ? `Showing ${existingCards} recommendation${existingCards === 1 ? '' : 's'}`
    : 'Recommendation results';
  resultsHeading.focus();
}

function updateProgress() {
  if (!form || !progressEl) return;
  const answered = REQUIRED_FIELDS.reduce((count, name) => {
    const field = form.querySelector(`[name="${name}"]:checked`);
    return field ? count + 1 : count;
  }, 0);
  progressEl.setAttribute('aria-valuenow', String(answered));
  if (progressCountEl) {
    progressCountEl.textContent = answered;
  }
}
