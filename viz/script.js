const benchmarkSelect = document.getElementById('benchmark-select');
const iterationSelect = document.getElementById('iteration-select');
const chartContainer = document.getElementById("chart-container");
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
        option.value = dir.replace('/', '');
        option.textContent = dir.replace('/', '');
        benchmarkSelect.appendChild(option);
    });
}

// Load iterations (JSON files) for a selected benchmark
async function loadIterations(benchmarkDir) {
    const last_iteration_select = iterationSelect.value;
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
    let has_last_selection = false;
    files.forEach(file => {
        const option = document.createElement('option');
        option.value = file;
        option.textContent = file;
        iterationSelect.appendChild(option);
        if (file === last_iteration_select) {
            has_last_selection = true;
        }
    });

    if (has_last_selection) {
        iterationSelect.value = last_iteration_select;
    }

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

    renderChart(benchmark, data);
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
    const total_times = [];

    for (const file of files) {
        const res = await fetch(`benchmark_results/${benchmark}/${file}`);
        const data = await res.json();
        const total_time = data.benchmarks.map(b => b.real_time / 1000000).reduce((sum, n) => sum + n);

        const l_year = file.substring(2, 4);
        const l_month = file.substring(5, 6);
        const l_day = file.substring(7, 8);
        const commit_hash = file.split(".")[0].split("_")[2];

        labels.push(`${l_day}.${l_month}.${l_year} [${commit_hash}]`);
        total_times.push(total_time);
    }

    chartContainer.innerHTML = "";
    if (chartInstance) chartInstance.destroy();
    const canvas = document.createElement("canvas");
    chartContainer.appendChild(canvas);

    chartInstance = new Chart(canvas, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: "Total Execution Time",
                data: total_times,
                fill: false,
                backgroundColor: "rgba(54, 162, 235, 0.2)",
                borderColor: 'rgb(54, 162, 235)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: "Experiment ID"
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: "Latency (ms)"
                    }
                }
            }
        }
    });
}

// Draw chart
function renderChart(benchmark, data) {
    chartContainer.innerHTML = "";
    if (chartInstance) chartInstance.destroy();

    if (benchmark.includes("min_hash")) {
        renderMinHashChart(data);
        return;
    }
    if (benchmark === "omni_insert") {
        renderOmniInsertChart(data);
        return;
    }
    if (benchmark === "omni_probe") {
        renderOmniProbeChart(data);
        return;
    }
    if (benchmark === "ssb") {
        renderSSBCharts(data);
    }
}

function renderMinHashChart(data) {
    const labels = data.benchmarks.filter(b => b.name.includes("Tree")).map(b => Number(b.name.split("/")[2]));
    const tree_vals = data.benchmarks.filter(b => b.name.includes("Tree")).map(b => b.real_time / 1000000.0);
    const vec_vals = data.benchmarks.filter(b => b.name.includes("Vector")).map(b => b.real_time / 1000000.0);

    const canvas = document.createElement("canvas");
    chartContainer.appendChild(canvas);

    chartInstance = new Chart(canvas, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: "RB-Tree Representation",
                data: tree_vals,
                fill: false,
                borderColor: 'green',
            }, {
                label: "Vector Representation",
                data: vec_vals,
                fill: false,
                borderColor: 'blue',
            }]
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: "Sketch Size"
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: "Latency (ms)"
                    }
                }
            }
        }
    });
}

function renderOmniInsertChart(data) {
    const labels = data.benchmarks.map((b, i) => (i + 1) * Number(b.name.split("/")[3].split(":")[1]));
    const values = data.benchmarks.map(b => b.items_per_second);

    const canvas = document.createElement("canvas");
    chartContainer.appendChild(canvas);
    chartInstance = new Chart(canvas, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: "Insertion Rate",
                data: values,
                fill: false,
                borderColor: 'green',
            }]
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: "Record Index"
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: "Throughput (Records/s)"
                    }
                }
            }
        }
    });
}

function renderOmniProbeChart(data) {
    const labels = data.benchmarks.filter(b => b.name.includes("PointQueryFlattened")).map(b => Number(b.name.split("/")[2]));
    const tree_vals = data.benchmarks.filter(b => b.name.split("/")[1] === "PointQuery").map(b => b.items_per_second);
    const vec_vals = data.benchmarks.filter(b => b.name.split("/")[1] === "PointQueryFlattened").map(b => b.items_per_second);

    const canvas = document.createElement("canvas");
    chartContainer.appendChild(canvas);
    chartInstance = new Chart(canvas, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: "RB-Tree Representation",
                data: tree_vals,
                fill: false,
                borderColor: 'green',
            }, {
                label: "Vector Representation",
                data: vec_vals,
                fill: false,
                borderColor: 'blue',
            }]
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: "Sketch Size"
                    }
                },
                y: {
                    title: {
                        beginAtZero: true,
                        display: true,
                        text: "Throughput (Probes/s)"
                    }
                }
            }
        }
    });
}

function renderSSBCharts(data) {
    const labels = data.benchmarks.map(b => b.name.split("/")[2][0] + "." + b.name.split("/")[2][1]);
    const values_latencies = data.benchmarks.map(b => b.real_time / 1000000.0);
    const values_q_errors = data.benchmarks.map(b => b.QErr)

    const canvas_latencies = document.createElement("canvas");
    const canvas_q_errors = document.createElement("canvas");

    chartContainer.appendChild(canvas_latencies);
    chartContainer.appendChild(canvas_q_errors);

    chartInstance = new Chart(canvas_latencies, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: "Query Estimation Latency (ms)",
                data: values_latencies,
                fill: false,
                backgroundColor: "rgba(54, 162, 235, 0.2)",
                borderColor: 'rgb(54, 162, 235)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: "Query ID"
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: "Latency (ms)"
                    }
                }
            }
        }
    });
    chartInstance = new Chart(canvas_q_errors, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: "Query Estimation Latency (ms)",
                data: values_q_errors,
                fill: false,
                backgroundColor: "rgba(75, 192, 192, 0.2)",
                borderColor: 'rgb(75, 192, 192)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: "Query ID"
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: "Q-Error"
                    }
                }
            },
            annotation: {
                annotations: [{
                    type: 'line',
                    mode: 'horizontal',
                    scaleID: 'y-axis-0',
                    value: 1,
                    borderColor: 'rgb(75, 192, 192)',
                    borderWidth: 4,
                    label: {
                        enabled: false,
                        content: 'Actual Cardinality'
                    }
                }]
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
