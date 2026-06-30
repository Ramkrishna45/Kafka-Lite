let prevPublished = 0;
let prevConsumed = 0;
let lastTimestamp = Date.now();
let isFirstLoad = true;

function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

async function fetchData() {
    try {
        const [metricsRes, diskRes] = await Promise.all([
            fetch('/api/metrics'),
            fetch('/api/disk')
        ]);
        
        const data = await metricsRes.json();
        const diskData = await diskRes.json();
        
        if (data.error) throw new Error(data.error);
        
        updateUI(data.metrics, data.groups, diskData);
        
        document.getElementById('connection-status').innerText = 'Connected';
        document.querySelector('.dot').classList.remove('offline');
        
    } catch (err) {
        console.error("Dashboard Fetch Error:", err);
        document.getElementById('connection-status').innerText = 'Offline';
        document.querySelector('.dot').classList.add('offline');
    }
}

function updateUI(metrics, groups, diskData) {
    if (!metrics) return;
    
    // Overview
    document.getElementById('val-topics').innerText = metrics.topics || 0;
    document.getElementById('val-partitions').innerText = metrics.partitions || 0;
    document.getElementById('val-disk').innerText = formatBytes(diskData.diskUsageBytes || 0);
    
    // Throughput Calculation
    const now = Date.now();
    const dt = (now - lastTimestamp) / 1000;
    
    if (dt > 0) {
        const pubDiff = metrics.messagesPublished - prevPublished;
        const subDiff = metrics.messagesConsumed - prevConsumed;
        
        if (!isFirstLoad) {
            const tpsPub = Math.max(0, Math.round(pubDiff / dt));
            const tpsSub = Math.max(0, Math.round(subDiff / dt));
            
            document.getElementById('val-tps-pub').innerText = tpsPub.toLocaleString();
            document.getElementById('val-tps-sub').innerText = tpsSub.toLocaleString();
        }
    }
    
    document.getElementById('val-total-pub').innerText = (metrics.messagesPublished || 0).toLocaleString();
    document.getElementById('val-total-sub').innerText = (metrics.messagesConsumed || 0).toLocaleString();
    
    prevPublished = metrics.messagesPublished || 0;
    prevConsumed = metrics.messagesConsumed || 0;
    lastTimestamp = now;
    isFirstLoad = false;
    
    // Update Table
    updateTable(metrics.consumerLag, groups);
}

function updateTable(consumerLag, groupsData) {
    const tbody = document.querySelector('#lag-table tbody');
    tbody.innerHTML = '';
    
    if (!groupsData || Object.keys(groupsData).length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--text-muted);">No active consumer groups</td></tr>';
        return;
    }
    
    for (const [groupId, group] of Object.entries(groupsData)) {
        if (!group.assignments) continue;
        
        for (const [topic, assignments] of Object.entries(group.assignments)) {
            for (const [partitionStr, consumerId] of Object.entries(assignments)) {
                
                const lagKey = `${topic}_partition${partitionStr}`;
                let lag = 0;
                if (consumerLag && consumerLag[groupId] && consumerLag[groupId][lagKey] !== undefined) {
                    lag = consumerLag[groupId][lagKey];
                }
                
                let lagClass = 'lag-ok';
                if (lag > 1000) lagClass = 'lag-danger';
                else if (lag > 100) lagClass = 'lag-warn';
                
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td><strong>${groupId}</strong></td>
                    <td>${topic}</td>
                    <td>${partitionStr}</td>
                    <td><span style="background: rgba(255,255,255,0.1); padding: 4px 8px; border-radius: 4px; font-family: monospace;">${consumerId}</span></td>
                    <td class="${lagClass}">${lag.toLocaleString()}</td>
                `;
                tbody.appendChild(tr);
            }
        }
    }
}

// Start polling
setInterval(fetchData, 1000);
fetchData();
