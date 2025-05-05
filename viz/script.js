const benchmarkSelect = document.getElementById('benchmark-select');
const iterationSelect = document.getElementById('iteration-select');
const chartCanvas = document.getElementById('benchmarkChart');
let chartInstance = null;

function median(arr) {
    const sorted = arr.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 !== 0
        ? sorted[mid]
        : (sorted[mid - 1] + sorted[mid]) / 2;
}

// Load benchmark directories
async function loadBenchmarks() {
    const response = await fetch('benchmark_results/');
    const text = await response.text();
    const parser = new DOMParser();
    const doc = parser.parseFromString(text, 'text/html');
    const dirs = Array.from(doc.querySelectorAll('a')).map(a => a.getAttribute('href'))
        .filter(name => name.endsWith('/') && name !== '../');
    dirs.forEach(dir => {
        const option = document.createElement('option');
        option.value = dir;
        option.textContent = dir.replace('/', '');
        benchmarkSelect.appendChild(option);
    });
}

// Load iterations (JSON files) for a selected benchmark
async function loadIterations(benchmarkDir) {
    iterationSelect.innerHTML = '';

    const response = await fetch(`benchmark_results/${benchmarkDir}`);
    const text = await response.text();
    const parser = new DOMParser();
    const doc = parser.parseFromString(text, 'text/html');
    const files = Array.from(doc.querySelectorAll('a'))
        .map(a => a.getAttribute('href'))
        .filter(name => name.endsWith('.json'))
        .sort();

    // Add "Combined" option
    const combinedOption = document.createElement('option');
    combinedOption.value = '__combined__';
    combinedOption.textContent = 'Combined (Median per Iteration)';
    iterationSelect.appendChild(combinedOption);

    // Add individual files
    files.forEach(file => {
        const option = document.createElement('option');
        option.value = file;
        option.textContent = file;
        iterationSelect.appendChild(option);
    });

    // Automatically trigger the plot for the default (Combined) selection
    const benchmark = benchmarkSelect.value;
    const iterationFile = iterationSelect.value;
    if (benchmark && iterationFile) {
        plotBenchmark(benchmark, iterationFile);
    }
}

// Handle plot request for individual or combined view
async function plotBenchmark(benchmark, iterationFile) {
    if (iterationFile === '__combined__') {
        await plotCombinedBenchmark(benchmark);
        return;
    }

    const url = `benchmark_results/${benchmark}/${iterationFile}`;
    const response = await fetch(url);
    const data = await response.json();

    const labels = data.benchmarks.map(b => b.name);
    const values = data.benchmarks.map(b => b.real_time);

    renderChart(labels, values, `Benchmark: ${iterationFile}`);
}

async function plotCombinedBenchmark(benchmark) {
    const response = await fetch(`benchmark_results/${benchmark}`);
    const text = await response.text();
    const parser = new DOMParser();
    const doc = parser.parseFromString(text, 'text/html');
    const files = Array.from(doc.querySelectorAll('a'))
        .map(a => a.getAttribute('href'))
        .filter(name => name.endsWith('.json'))
        .sort();

    const labels = [];
    const medianValues = [];

    for (const file of files) {
        const res = await fetch(`benchmark_results/${benchmark}/${file}`);
        const data = await res.json();
        const times = data.benchmarks.map(b => b.real_time);
        if (times.length > 0) {
            labels.push(file);
            medianValues.push(median(times));
        }
    }

    renderChart(labels, medianValues, 'Benchmark: Median per Iteration');
}

// Draw chart
function renderChart(labels, values, label) {
    if (chartInstance) chartInstance.destroy();

    chartInstance = new Chart(chartCanvas, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: label,
                data: values,
                fill: false,
                borderColor: 'green',
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    title: {
                        display: true,
                        text: 'Real Time (ns)'
                    }
                }
            }
        }
    });
}

// Event listeners
benchmarkSelect.addEventListener('change', () => {
    const benchmark = benchmarkSelect.value;
    if (benchmark) {
        loadIterations(benchmark);
    }
});

iterationSelect.addEventListener('change', () => {
    const benchmark = benchmarkSelect.value;
    const iterationFile = iterationSelect.value;
    if (benchmark && iterationFile) {
        plotBenchmark(benchmark, iterationFile);
    }
});

// Init
loadBenchmarks();
