'use strict';

(() => {
  const nav = document.querySelector('[data-nav]');
  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  const toggleNavState = () => {
    if (!nav) {
      return;
    }
    if (window.scrollY > 32) {
      nav.classList.add('nav-condensed');
    } else {
      nav.classList.remove('nav-condensed');
    }
  };

  toggleNavState();
  window.addEventListener('scroll', toggleNavState);

  const scrollLinks = document.querySelectorAll('[data-scroll]');
  scrollLinks.forEach((link) => {
    link.addEventListener('click', (event) => {
      const href = link.getAttribute('href');
      if (!href || !href.startsWith('#')) {
        return;
      }
      const target = document.querySelector(href);
      if (!target) {
        return;
      }
      event.preventDefault();
      const top = target.getBoundingClientRect().top + window.scrollY - 32;
      if (prefersReducedMotion) {
        window.scrollTo(0, top);
        return;
      }
      window.scrollTo({
        top,
        behavior: 'smooth',
      });
    });
  });

  if (prefersReducedMotion) {
    document.querySelectorAll('[data-animate]').forEach((block) => {
      block.classList.add('is-visible');
    });
    return;
  }

  const observer = new IntersectionObserver(
    (entries, obs) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) {
          return;
        }
        entry.target.classList.add('is-visible');
        obs.unobserve(entry.target);
      });
    },
    {
      threshold: 0.2,
      rootMargin: '0px 0px -60px 0px',
    },
  );

  document.querySelectorAll('[data-animate]').forEach((block) => {
    observer.observe(block);
  });
})();


