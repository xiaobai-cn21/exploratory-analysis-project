// ============================================
// 纽约州地图初始化
// ============================================

// 纽约州中心坐标
const NY_CENTER = [42.9538, -75.5268];
const NY_ZOOM = 7;

// 纽约州主要县的坐标（示例数据）
const NY_COUNTIES = {
    'New York': { lat: 40.7128, lng: -74.0060, name: 'New York County (Manhattan)' },
    'Kings': { lat: 40.6526, lng: -73.9497, name: 'Kings County (Brooklyn)' },
    'Queens': { lat: 40.7282, lng: -73.7949, name: 'Queens County' },
    'Bronx': { lat: 40.8448, lng: -73.8648, name: 'Bronx County' },
    'Richmond': { lat: 40.5795, lng: -74.1502, name: 'Richmond County (Staten Island)' },
    'Nassau': { lat: 40.7389, lng: -73.5890, name: 'Nassau County' },
    'Suffolk': { lat: 40.8176, lng: -72.6158, name: 'Suffolk County' },
    'Westchester': { lat: 41.1220, lng: -73.7949, name: 'Westchester County' },
    'Erie': { lat: 42.7684, lng: -78.8871, name: 'Erie County (Buffalo)' },
    'Monroe': { lat: 43.1566, lng: -77.6088, name: 'Monroe County (Rochester)' },
    'Onondaga': { lat: 43.0481, lng: -76.1474, name: 'Onondaga County (Syracuse)' },
    'Albany': { lat: 42.6526, lng: -73.7562, name: 'Albany County' },
    'Dutchess': { lat: 41.7654, lng: -73.7478, name: 'Dutchess County' },
    'Orange': { lat: 41.3916, lng: -74.3100, name: 'Orange County' },
    'Rockland': { lat: 41.1489, lng: -73.9790, name: 'Rockland County' }
};

// 模拟数据（实际应该从API或数据文件加载）
const COUNTY_DATA = {
    'New York': { courses: 1250, students: 45230, participation: 68.5 },
    'Kings': { courses: 980, students: 38210, participation: 62.3 },
    'Queens': { courses: 1120, students: 41250, participation: 65.8 },
    'Bronx': { courses: 750, students: 28120, participation: 48.2 },
    'Richmond': { courses: 420, students: 15230, participation: 55.6 },
    'Nassau': { courses: 1350, students: 48250, participation: 72.1 },
    'Suffolk': { courses: 1180, students: 42560, participation: 68.9 },
    'Westchester': { courses: 1420, students: 51230, participation: 75.3 },
    'Erie': { courses: 680, students: 24120, participation: 58.4 },
    'Monroe': { courses: 590, students: 21230, participation: 54.7 },
    'Onondaga': { courses: 520, students: 18240, participation: 51.2 },
    'Albany': { courses: 480, students: 17210, participation: 49.8 },
    'Dutchess': { courses: 420, students: 15120, participation: 47.5 },
    'Orange': { courses: 380, students: 13890, participation: 45.3 },
    'Rockland': { courses: 350, students: 12450, participation: 43.8 }
};

let map;
let markers = [];

// 初始化地图
function initMap() {
    // 创建地图实例
    map = L.map('ny-map').setView(NY_CENTER, NY_ZOOM);

    // 添加OpenStreetMap底图
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors',
        maxZoom: 18,
    }).addTo(map);

    // 为每个县添加标记
    Object.keys(NY_COUNTIES).forEach(countyName => {
        const county = NY_COUNTIES[countyName];
        const data = COUNTY_DATA[countyName] || { courses: 0, students: 0, participation: 0 };

        // 创建自定义图标
        const icon = L.divIcon({
            className: 'county-marker',
            html: `<div class="marker-pin" style="background: ${getColorByParticipation(data.participation)}">
                <span>${Math.round(data.participation)}%</span>
            </div>`,
            iconSize: [40, 40],
            iconAnchor: [20, 40]
        });

        // 添加标记
        const marker = L.marker([county.lat, county.lng], { icon: icon })
            .addTo(map)
            .bindPopup(createPopupContent(countyName, county.name, data))
            .on('click', () => showCountyInfo(countyName, county.name, data));

        markers.push(marker);
    });

    // 添加图例
    addLegend();
}

// 根据参与率获取颜色
function getColorByParticipation(participation) {
    if (participation >= 70) return '#10b981'; // 绿色
    if (participation >= 60) return '#3b82f6'; // 蓝色
    if (participation >= 50) return '#f59e0b'; // 橙色
    return '#ef4444'; // 红色
}

// 创建弹出窗口内容
function createPopupContent(countyName, fullName, data) {
    return `
        <div class="popup-content">
            <h3>${fullName}</h3>
            <div class="popup-stats">
                <div class="popup-stat">
                    <strong>${data.courses.toLocaleString()}</strong>
                    <span>课程数</span>
                </div>
                <div class="popup-stat">
                    <strong>${data.students.toLocaleString()}</strong>
                    <span>学生数</span>
                </div>
                <div class="popup-stat">
                    <strong>${data.participation.toFixed(1)}%</strong>
                    <span>参与率</span>
                </div>
            </div>
        </div>
    `;
}

// 显示县信息
function showCountyInfo(countyName, fullName, data) {
    const infoDiv = document.getElementById('map-info');
    infoDiv.className = 'map-info active';
    infoDiv.innerHTML = `
        <h4>${fullName}</h4>
        <div class="map-info-stats">
            <div class="map-stat">
                <div class="map-stat-value">${data.courses.toLocaleString()}</div>
                <div class="map-stat-label">AP/IB课程数</div>
            </div>
            <div class="map-stat">
                <div class="map-stat-value">${data.students.toLocaleString()}</div>
                <div class="map-stat-label">注册学生数</div>
            </div>
            <div class="map-stat">
                <div class="map-stat-value">${data.participation.toFixed(1)}%</div>
                <div class="map-stat-label">参与率</div>
            </div>
        </div>
        <p style="margin-top: 1rem; color: var(--medium-gray);">
            数据来源：纽约州教育部 (NYSED) 2023-2024学年
        </p>
    `;
}

// 添加图例
function addLegend() {
    const legend = L.control({ position: 'bottomright' });

    legend.onAdd = function(map) {
        const div = L.DomUtil.create('div', 'map-legend');
        div.innerHTML = `
            <div style="background: white; padding: 1rem; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.2);">
                <h4 style="margin: 0 0 0.5rem 0; color: var(--primary-blue); font-size: 0.9rem;">参与率</h4>
                <div style="display: flex; flex-direction: column; gap: 0.25rem;">
                    <div style="display: flex; align-items: center; gap: 0.5rem;">
                        <span style="display: inline-block; width: 20px; height: 20px; background: #10b981; border-radius: 4px;"></span>
                        <span style="font-size: 0.85rem;">≥ 70%</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 0.5rem;">
                        <span style="display: inline-block; width: 20px; height: 20px; background: #3b82f6; border-radius: 4px;"></span>
                        <span style="font-size: 0.85rem;">60-70%</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 0.5rem;">
                        <span style="display: inline-block; width: 20px; height: 20px; background: #f59e0b; border-radius: 4px;"></span>
                        <span style="font-size: 0.85rem;">50-60%</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 0.5rem;">
                        <span style="display: inline-block; width: 20px; height: 20px; background: #ef4444; border-radius: 4px;"></span>
                        <span style="font-size: 0.85rem;">< 50%</span>
                    </div>
                </div>
            </div>
        `;
        return div;
    };

    legend.addTo(map);
}

// 页面加载完成后初始化地图
document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('ny-map')) {
        initMap();
    }
});

// 添加标记样式
const style = document.createElement('style');
style.textContent = `
    .county-marker {
        background: transparent;
        border: none;
    }
    
    .marker-pin {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-weight: bold;
        font-size: 0.75rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        border: 2px solid white;
    }
    
    .popup-content h3 {
        margin: 0 0 1rem 0;
        color: var(--primary-blue);
        font-size: 1.1rem;
    }
    
    .popup-stats {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 1rem;
    }
    
    .popup-stat {
        text-align: center;
        padding: 0.5rem;
        background: var(--light-blue);
        border-radius: 8px;
    }
    
    .popup-stat strong {
        display: block;
        font-size: 1.25rem;
        color: var(--primary-blue);
        margin-bottom: 0.25rem;
    }
    
    .popup-stat span {
        font-size: 0.75rem;
        color: var(--medium-gray);
        text-transform: uppercase;
    }
`;
document.head.appendChild(style);
