document.addEventListener('DOMContentLoaded', function() {
    const form = document.getElementById('analysis-form');
    const resultContainer = document.getElementById('result');
    const loadingDiv = resultContainer.querySelector('.loading');
    const resultContent = resultContainer.querySelector('.result-content');
    const progressSpan = document.getElementById('progress');
    
    form.addEventListener('submit', async function(e) {
        e.preventDefault();
        
        const formData = {
            email: document.getElementById('email').value,
            business_name: document.getElementById('business_name').value,
            industry: document.getElementById('industry').value,
            location: document.getElementById('location').value,
            radius_miles: 10
        };
        
        resultContainer.style.display = 'block';
        loadingDiv.style.display = 'block';
        resultContent.style.display = 'none';
        progressSpan.textContent = '0';
        
        try {
            const startResponse = await fetch('/start-analysis', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(formData)
            });
            
            if (!startResponse.ok) {
                const error = await startResponse.json();
                throw new Error(error.detail || 'Failed to start analysis');
            }
            
            const job = await startResponse.json();
            const jobId = job.job_id;
            
            let completed = false;
            while (!completed) {
                await new Promise(resolve => setTimeout(resolve, 1500));
                
                const statusResponse = await fetch(`/jobs/${jobId}`);
                const status = await statusResponse.json();
                
                progressSpan.textContent = status.progress_percent || 0;
                
                if (status.status === 'completed') {
                    completed = true;
                    displayResults(jobId, status.result);
                } else if (status.status === 'failed') {
                    throw new Error(status.error || 'Analysis failed');
                }
            }
        } catch (error) {
            loadingDiv.innerHTML = `<p style="color: #dc2626;">Error: ${error.message}</p>`;
        }
    });
    
    function displayResults(jobId, result) {
        loadingDiv.style.display = 'none';
        resultContent.style.display = 'block';
        
        const summary = result.summary || {};
        const top10 = result.top_10 || [];
        const opportunities = summary.opportunities || [];
        
        let opportunitiesHtml = '';
        if (opportunities.length > 0) {
            opportunitiesHtml = `
                <div class="opportunities" style="margin-bottom: 24px;">
                    <h4 style="margin-bottom: 12px; color: #1e3a5f;">Key Opportunities</h4>
                    ${opportunities.map(opp => `
                        <div style="background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px; padding: 12px; margin-bottom: 8px;">
                            <strong style="color: #166534;">${(opp.type || '').replace(/_/g, ' ')}</strong>
                            <p style="margin: 4px 0; color: #166534;">${opp.insight}</p>
                            <p style="margin: 0; font-size: 0.9rem; color: #15803d;"><em>Action: ${opp.action}</em></p>
                        </div>
                    `).join('')}
                </div>
            `;
        }
        
        resultContent.innerHTML = `
            <h3>Analysis Complete!</h3>
            <div class="result-summary">
                <div class="stat-card">
                    <div class="stat-value">${summary.total_competitors || 0}</div>
                    <div class="stat-label">Competitors</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${(summary.average_rating || 0).toFixed(1)}</div>
                    <div class="stat-label">Avg Rating</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${Math.round(summary.average_reviews || 0)}</div>
                    <div class="stat-label">Avg Reviews</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">${(summary.market_saturation || 'N/A').charAt(0).toUpperCase() + (summary.market_saturation || 'N/A').slice(1)}</div>
                    <div class="stat-label">Saturation</div>
                </div>
            </div>
            
            ${opportunitiesHtml}
            
            <h4 style="margin-bottom: 12px; color: #1e3a5f;">Top Competitors</h4>
            <table class="competitors-table">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Business</th>
                        <th>Score</th>
                        <th>Rating</th>
                        <th>Reviews</th>
                    </tr>
                </thead>
                <tbody>
                    ${top10.slice(0, 5).map((b, i) => `
                        <tr>
                            <td>${i + 1}</td>
                            <td>${b.name || 'Unknown'}</td>
                            <td><strong>${b.score || 0}</strong></td>
                            <td>${(b.rating || 0).toFixed(1)}</td>
                            <td>${b.reviews || 0}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
            
            <div class="download-btns">
                <a href="/jobs/${jobId}/pdf" class="download-btn" download>
                    📄 Download PDF Report
                </a>
                <a href="/jobs/${jobId}/csv" class="download-btn secondary" download>
                    📊 Download CSV Data
                </a>
            </div>
        `;
    }
});
