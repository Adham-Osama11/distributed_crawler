
    document.addEventListener('DOMContentLoaded', function() {
        const searchInput = document.getElementById('search-input');
        const searchButton = document.getElementById('search-button');
        const resultsContainer = document.getElementById('results-container');
        
        // Search when button is clicked
        searchButton.addEventListener('click', performSearch);
        
        // Search when Enter key is pressed
        searchInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                performSearch();
            }
        });
        
        function performSearch() {
            const query = searchInput.value.trim();
            if (!query) return;
            
            // Show loading indicator
            resultsContainer.innerHTML = '<p>Searching...</p>';
            
            // Perform search request
            fetch('/api/search', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    query: query,
                    max_results: 20
                })
            })
            .then(response => response.json())
            .then(data => {
                displayResults(data);
            })
            .catch(error => {
                resultsContainer.innerHTML = `<p>Error: ${error.message}</p>`;
            });
        }
        
        function displayResults(data) {
            if (data.error) {
                resultsContainer.innerHTML = `<p>Error: ${data.error}</p>`;
                return;
            }
            
            if (!data.results || data.results.length === 0) {
                resultsContainer.innerHTML = `<p>No results found for "${data.query}"</p>`;
                return;
            }
            
            let html = `<h2>Search Results for "${data.query}" (${data.result_count} results)</h2>`;
            
            data.results.forEach((result, index) => {
                html += `
                <div class="result-item">
                    <div class="result-title">${index + 1}. ${result.title || 'No Title'}</div>
                    <div class="result-url"><a href="${result.url}" target="_blank">${result.url}</a></div>
                    <div class="result-description">${result.description || 'No description available'}</div>
                    <div class="result-score">Score: ${result.score.toFixed(2)}</div>
                </div>
                `;
            });
            
            resultsContainer.innerHTML = html;
        }
    });
    