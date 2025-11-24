// ============================================
// 导航栏滚动效果
// ============================================
const navbar = document.getElementById('navbar');
const navLinks = document.querySelectorAll('.nav-link');
const hamburger = document.getElementById('hamburger');
const navMenu = document.getElementById('navMenu');

// 监听滚动事件，改变导航栏样式
window.addEventListener('scroll', () => {
    if (window.scrollY > 50) {
        navbar.classList.add('scrolled');
    } else {
        navbar.classList.remove('scrolled');
    }
});

// 移动端菜单切换
hamburger.addEventListener('click', () => {
    navMenu.classList.toggle('active');
    hamburger.classList.toggle('active');
});

// 点击导航链接后关闭移动端菜单
navLinks.forEach(link => {
    link.addEventListener('click', () => {
        navMenu.classList.remove('active');
        hamburger.classList.remove('active');
    });
});

// ============================================
// 平滑滚动到锚点
// ============================================
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));
        if (target) {
            const offsetTop = target.offsetTop - 70; // 考虑导航栏高度
            window.scrollTo({
                top: offsetTop,
                behavior: 'smooth'
            });
        }
    });
});

// ============================================
// 滚动动画 - Intersection Observer
// ============================================
const observerOptions = {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
};

const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            entry.target.style.opacity = '1';
            entry.target.style.transform = 'translateY(0)';
        }
    });
}, observerOptions);

// 为需要动画的元素添加观察
const animateElements = document.querySelectorAll(
    '.about-card, .research-card, .flow-step, .timeline-item, .result-card'
);

animateElements.forEach(el => {
    el.style.opacity = '0';
    el.style.transform = 'translateY(30px)';
    el.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
    observer.observe(el);
});

// ============================================
// 数字计数动画
// ============================================
function animateCounter(element, target, duration = 2000) {
    let start = 0;
    const increment = target / (duration / 16);
    const timer = setInterval(() => {
        start += increment;
        if (start >= target) {
            element.textContent = formatNumber(target);
            clearInterval(timer);
        } else {
            element.textContent = formatNumber(Math.floor(start));
        }
    }, 16);
}

function formatNumber(num) {
    if (num >= 1000) {
        return num.toLocaleString('en-US');
    }
    return num.toString();
}

// 观察统计数字元素
const statNumbers = document.querySelectorAll('.stat-number');
const statObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting && !entry.target.classList.contains('animated')) {
            entry.target.classList.add('animated');
            const text = entry.target.textContent;
            // 处理不同的数字格式
            if (text.includes(',')) {
                const num = parseInt(text.replace(/,/g, ''));
                animateCounter(entry.target, num);
            } else if (text.includes('-')) {
                // 处理日期格式，不进行动画
                return;
            } else {
                const num = parseInt(text);
                if (!isNaN(num)) {
                    animateCounter(entry.target, num);
                }
            }
        }
    });
}, { threshold: 0.5 });

statNumbers.forEach(stat => {
    statObserver.observe(stat);
});

// ============================================
// 时间线进度指示
// ============================================
const timelineItems = document.querySelectorAll('.timeline-item');
const timelineObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            entry.target.classList.add('visible');
        }
    });
}, { threshold: 0.3 });

timelineItems.forEach(item => {
    timelineObserver.observe(item);
});

// ============================================
// 卡片悬停效果增强
// ============================================
const cards = document.querySelectorAll('.about-card, .research-card, .result-card');
cards.forEach(card => {
    card.addEventListener('mouseenter', function() {
        this.style.transition = 'all 0.3s ease';
    });
});

// ============================================
// 算法标签随机颜色效果（可选）
// ============================================
const algorithmTags = document.querySelectorAll('.algorithm-tag');
const colors = [
    'var(--primary-blue)',
    'var(--secondary-blue)',
    'var(--accent-blue)'
];

algorithmTags.forEach((tag, index) => {
    tag.addEventListener('mouseenter', function() {
        const randomColor = colors[Math.floor(Math.random() * colors.length)];
        this.style.background = randomColor;
    });
    
    tag.addEventListener('mouseleave', function() {
        this.style.background = 'var(--primary-blue)';
    });
});

// ============================================
// 页面加载完成后的初始化
// ============================================
window.addEventListener('DOMContentLoaded', () => {
    // 添加加载动画类
    document.body.classList.add('loaded');
    
    // 初始化导航栏状态
    if (window.scrollY > 50) {
        navbar.classList.add('scrolled');
    }
    
    // 控制台输出项目信息
    console.log('%c🗽 纽约州教育数据分析项目', 'color: #003DA5; font-size: 20px; font-weight: bold;');
    console.log('%c基于数据驱动的教育公平性研究', 'color: #6B7280; font-size: 12px;');
});

// ============================================
// 返回顶部按钮（可选功能）
// ============================================
let scrollTopBtn = null;

function createScrollTopButton() {
    scrollTopBtn = document.createElement('button');
    scrollTopBtn.innerHTML = '↑';
    scrollTopBtn.className = 'scroll-top-btn';
    scrollTopBtn.style.cssText = `
        position: fixed;
        bottom: 30px;
        right: 30px;
        width: 50px;
        height: 50px;
        border-radius: 50%;
        background: var(--primary-blue);
        color: white;
        border: none;
        font-size: 24px;
        cursor: pointer;
        opacity: 0;
        visibility: hidden;
        transition: all 0.3s ease;
        z-index: 999;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    `;
    
    document.body.appendChild(scrollTopBtn);
    
    scrollTopBtn.addEventListener('click', () => {
        window.scrollTo({
            top: 0,
            behavior: 'smooth'
        });
    });
    
    window.addEventListener('scroll', () => {
        if (window.scrollY > 300) {
            scrollTopBtn.style.opacity = '1';
            scrollTopBtn.style.visibility = 'visible';
        } else {
            scrollTopBtn.style.opacity = '0';
            scrollTopBtn.style.visibility = 'hidden';
        }
    });
}

// 创建返回顶部按钮
createScrollTopButton();

// ============================================
// 响应式处理
// ============================================
function handleResize() {
    // 如果窗口宽度大于768px，确保移动端菜单关闭
    if (window.innerWidth > 768) {
        navMenu.classList.remove('active');
        hamburger.classList.remove('active');
    }
}

window.addEventListener('resize', handleResize);

// ============================================
// 性能优化：防抖函数
// ============================================
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// 优化滚动事件
const optimizedScrollHandler = debounce(() => {
    if (window.scrollY > 50) {
        navbar.classList.add('scrolled');
    } else {
        navbar.classList.remove('scrolled');
    }
}, 10);

window.addEventListener('scroll', optimizedScrollHandler);
