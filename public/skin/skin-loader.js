(() => {
  'use strict';

  const STORAGE_KEY = 'qip-active-skin';
  const DEFAULT_SKIN = 'default';
  const KNOWN_SKINS = ['default', 'qip2005', 'infium', 'classic', 'dark'];
  let activeLink = null;

  function safeSkinId(id) {
    return KNOWN_SKINS.includes(id) ? id : DEFAULT_SKIN;
  }

  function removeSkinClasses() {
    [...document.body.classList]
      .filter(name => name.startsWith('skin-'))
      .forEach(name => document.body.classList.remove(name));
  }

  function load(id, options = {}) {
    const skinId = safeSkinId(id);
    const href = `/skin/${skinId}/skin.css?v=2`;

    if (!activeLink) {
      activeLink = document.getElementById('qip-active-skin-css');
    }
    if (!activeLink) {
      activeLink = document.createElement('link');
      activeLink.id = 'qip-active-skin-css';
      activeLink.rel = 'stylesheet';
      document.head.appendChild(activeLink);
    }

    activeLink.href = href;
    removeSkinClasses();
    document.body.classList.add(`skin-${skinId}`);
    document.documentElement.dataset.qipSkin = skinId;

    if (options.persist !== false) {
      localStorage.setItem(STORAGE_KEY, skinId);
    }

    window.dispatchEvent(new CustomEvent('qip:skinchange', {
      detail: { id: skinId, href }
    }));

    return skinId;
  }

  async function getManifest(id) {
    const skinId = safeSkinId(id);
    const response = await fetch(`/skin/${skinId}/manifest.json`, { cache: 'no-store' });
    if (!response.ok) throw new Error(`Skin manifest unavailable: ${skinId}`);
    return response.json();
  }

  function current() {
    return document.documentElement.dataset.qipSkin || DEFAULT_SKIN;
  }

  window.QIPSkin = Object.freeze({
    load,
    current,
    getManifest,
    list: () => [...KNOWN_SKINS],
    reset: () => load(DEFAULT_SKIN)
  });

  const saved = safeSkinId(localStorage.getItem(STORAGE_KEY) || DEFAULT_SKIN);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => load(saved, { persist: false }), { once: true });
  } else {
    load(saved, { persist: false });
  }
})();
