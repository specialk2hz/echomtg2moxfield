from flask import Flask, render_template, send_file
from data_processing import fetch_and_process_data  # Import the processing function

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')  # Render the main page

@app.route('/run', methods=['POST'])
def run_app():
    try:
        output_file = fetch_and_process_data()  # Fetch and process data
        return send_file(output_file, as_attachment=True)  # Download the file
    except Exception as e:
        return str(e)  # Return the error message

if __name__ == '__main__':
    app.run(debug=True)
