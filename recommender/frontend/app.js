// API URL
const API_URL = 'https://music-recommender-api-316327352106.us-central1.run.app';

// Elementos del DOM
const tabs = document.querySelectorAll('.tab-button');
const tabContents = document.querySelectorAll('.tab-content');
const trackNameInput = document.getElementById('trackNameInput');
const semanticInput = document.getElementById('semanticInput');
const searchByNameBtn = document.getElementById('searchByNameBtn');
const searchSemanticBtn = document.getElementById('searchSemanticBtn');
const clearNameBtn = document.getElementById('clearNameBtn');
const clearSemanticBtn = document.getElementById('clearSemanticBtn');
const suggestionsDiv = document.getElementById('suggestions');
const loadingDiv = document.getElementById('loading');
const errorDiv = document.getElementById('error');
const resultsDiv = document.getElementById('results');
const paginationDiv = document.getElementById('pagination');
const prevPageBtn = document.getElementById('prevPage');
const nextPageBtn = document.getElementById('nextPage');
const pageInfo = document.getElementById('pageInfo');
const exampleTags = document.querySelectorAll('.tag');

// Cache de canciones
let allTracks = [];

// Paginación
let currentResults = [];
let currentPage = 1;
const resultsPerPage = 6;
let lastQuery = null;
let lastSearchType = null;

// ============================================================================
// INICIALIZACIÓN
// ============================================================================

// Cargar todas las canciones al inicio
async function loadAllTracks() {
    try {
        const response = await fetch(`${API_URL}/tracks?limit=100`);
        const data = await response.json();
        allTracks = data.tracks;
        console.log('Canciones cargadas:', allTracks.length);
    } catch (error) {
        console.error('Error cargando canciones:', error);
    }
}

// Cargar al inicio
loadAllTracks();

// ============================================================================
// BOTONES LIMPIAR
// ============================================================================

clearNameBtn.addEventListener('click', () => {
    trackNameInput.value = '';
    suggestionsDiv.classList.remove('show');
    clearResults();
    clearError();
    currentResults = [];
    currentPage = 1;
});

clearSemanticBtn.addEventListener('click', () => {
    semanticInput.value = '';
    clearResults();
    clearError();
    currentResults = [];
    currentPage = 1;
});

// ============================================================================
// TABS
// ============================================================================

tabs.forEach(tab => {
    tab.addEventListener('click', () => {
        const tabName = tab.dataset.tab;
        
        // Actualizar tabs
        tabs.forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        
        // Actualizar contenido
        tabContents.forEach(content => {
            content.classList.remove('active');
        });
        document.getElementById(`${tabName}-search`).classList.add('active');
        
        // Limpiar resultados y errores
        clearResults();
    });
});

// ============================================================================
// BÚSQUEDA POR NOMBRE
// ============================================================================

// Autocompletado
trackNameInput.addEventListener('input', (e) => {
    const query = e.target.value.trim().toLowerCase();
    
    if (query.length < 2) {
        suggestionsDiv.classList.remove('show');
        return;
    }
    
    // Filtrar canciones
    const matches = allTracks.filter(track => 
        track.track_name.toLowerCase().includes(query) ||
        track.artist_name.toLowerCase().includes(query)
    ).slice(0, 5);
    
    if (matches.length > 0) {
        showSuggestions(matches);
    } else {
        suggestionsDiv.classList.remove('show');
    }
});

function showSuggestions(tracks) {
    suggestionsDiv.innerHTML = tracks.map(track => `
        <div class="suggestion-item" data-track-id="${track.track_id}" data-track-name="${track.track_name}">
            <strong>${track.track_name}</strong> - ${track.artist_name}
        </div>
    `).join('');
    
    suggestionsDiv.classList.add('show');
    
    // Agregar event listeners
    document.querySelectorAll('.suggestion-item').forEach(item => {
        item.addEventListener('click', () => {
            const trackName = item.dataset.trackName;
            trackNameInput.value = trackName;
            suggestionsDiv.classList.remove('show');
            searchByName(trackName);
        });
    });
}

// Buscar al hacer click en el botón
searchByNameBtn.addEventListener('click', () => {
    const query = trackNameInput.value.trim();
    if (query) {
        searchByName(query);
    }
});

// Buscar al presionar Enter
trackNameInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        const query = trackNameInput.value.trim();
        if (query) {
            searchByName(query);
        }
    }
});

async function searchByName(trackName) {
    showLoading();
    clearError();
    suggestionsDiv.classList.remove('show');
    
    try {
        const response = await fetch(
            `${API_URL}/recommend?track_name=${encodeURIComponent(trackName)}&k=10`
        );
        
        if (!response.ok) {
            throw new Error('Canción no encontrada');
        }
        
        const data = await response.json();
        showResults(data, 'nombre');
        
    } catch (error) {
        showError(error.message || 'Error al buscar recomendaciones');
    } finally {
        hideLoading();
    }
}

// ============================================================================
// BÚSQUEDA SEMÁNTICA
// ============================================================================

// Buscar al hacer click en el botón
searchSemanticBtn.addEventListener('click', () => {
    const query = semanticInput.value.trim();
    if (query) {
        searchSemantic(query);
    }
});

// Buscar al presionar Enter
semanticInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        const query = semanticInput.value.trim();
        if (query) {
            searchSemantic(query);
        }
    }
});

// Ejemplos de búsqueda
exampleTags.forEach(tag => {
    tag.addEventListener('click', () => {
        const query = tag.dataset.query;
        semanticInput.value = query;
        searchSemantic(query);
    });
});

async function searchSemantic(query) {
    showLoading();
    clearError();
    
    try {
        const response = await fetch(
            `${API_URL}/recommend/semantic?query=${encodeURIComponent(query)}&k=10&min_similarity=0.2`,
            { method: 'POST' }
        );
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(
                errorData.detail || 
                'No se encontraron canciones. Intenta con una frase más descriptiva (ej: "canciones románticas", "música alegre")'
            );
        }
        
        const data = await response.json();
        
        if (!data.results || data.results.length === 0) {
            throw new Error('No se encontraron canciones. Intenta con una frase más descriptiva.');
        }
        
        showResults(data, 'semántica');
        
    } catch (error) {
        showError(error.message || 'Error en la búsqueda semántica');
    } finally {
        hideLoading();
    }
}

// ============================================================================
// MOSTRAR RESULTADOS
// ============================================================================

function showResults(data, searchType) {
    const query = data.query;
    const results = data.results || data.recommendations;
    
    if (!results || results.length === 0) {
        showError('No se encontraron resultados');
        return;
    }
    
    // Guardar resultados y resetear página
    currentResults = results;
    currentPage = 1;
    lastQuery = query;
    lastSearchType = searchType;
    
    // Mostrar primera página
    displayPage(query, searchType);
}

function displayPage(query, searchType) {
    const totalPages = Math.ceil(currentResults.length / resultsPerPage);
    const startIndex = (currentPage - 1) * resultsPerPage;
    const endIndex = startIndex + resultsPerPage;
    const pageResults = currentResults.slice(startIndex, endIndex);
    
    // Header
    let headerHTML = '';
    if (searchType === 'nombre') {
        headerHTML = `
            <div class="results-header">
                <h2>Canciones similares a "${query.track_name || query}"</h2>
                <p class="results-count">${currentResults.length} recomendaciones encontradas</p>
            </div>
        `;
    } else {
        headerHTML = `
            <div class="results-header">
                <h2>Resultados para: "${query}"</h2>
                <p class="results-count">${currentResults.length} canciones encontradas</p>
            </div>
        `;
    }
    
    // Cards de la página actual
    const cardsHTML = pageResults.map((track, index) => {
        const absoluteIndex = startIndex + index;
        const similarity = Math.round(track.similarity_score * 100);
        
        return `
            <div class="card">
                <div class="card-header">
                    <div class="rank">#${absoluteIndex + 1}</div>
                    <div class="card-info">
                        <div class="track-name">${track.track_name}</div>
                        <div class="artist-name">${track.artist_name}</div>
                    </div>
                </div>
                <div class="similarity-bar">
                    <div class="similarity-fill" style="width: ${similarity}%"></div>
                </div>
                <div class="similarity-score">
                    ${similarity}% similar
                </div>
            </div>
        `;
    }).join('');
    
    resultsDiv.innerHTML = `
        ${headerHTML}
        <div class="results-grid">
            ${cardsHTML}
        </div>
    `;
    
    // Mostrar/ocultar paginación
    if (totalPages > 1) {
        showPagination(totalPages);
    } else {
        hidePagination();
    }
}

function showPagination(totalPages) {
    paginationDiv.classList.remove('hidden');
    pageInfo.textContent = `Página ${currentPage} de ${totalPages}`;
    
    // Habilitar/deshabilitar botones
    prevPageBtn.disabled = currentPage === 1;
    nextPageBtn.disabled = currentPage === totalPages;
}

function hidePagination() {
    paginationDiv.classList.add('hidden');
}

// Event listeners de paginación
prevPageBtn.addEventListener('click', () => {
    if (currentPage > 1) {
        currentPage--;
        displayPage(lastQuery, lastSearchType);
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }
});

nextPageBtn.addEventListener('click', () => {
    const totalPages = Math.ceil(currentResults.length / resultsPerPage);
    if (currentPage < totalPages) {
        currentPage++;
        displayPage(lastQuery, lastSearchType);
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }
});

// ============================================================================
// UTILIDADES
// ============================================================================

function showLoading() {
    loadingDiv.classList.remove('hidden');
    resultsDiv.innerHTML = '';
}

function hideLoading() {
    loadingDiv.classList.add('hidden');
}

function showError(message) {
    errorDiv.textContent = message;
    errorDiv.classList.remove('hidden');
    resultsDiv.innerHTML = '';
}

function clearError() {
    errorDiv.classList.add('hidden');
}

function clearResults() {
    resultsDiv.innerHTML = '';
    hidePagination();
    clearError();
}

// Cerrar sugerencias al hacer click fuera
document.addEventListener('click', (e) => {
    if (!trackNameInput.contains(e.target) && !suggestionsDiv.contains(e.target)) {
        suggestionsDiv.classList.remove('show');
    }
});