from flask import Flask, render_template, request

app = Flask(__name__)

#Criar a primeira pagina do site
# route -> caminho que vem após o inicio da url
#   exemplo = meusite.com/usuario

#decorator em py atribui uma funcionalidade à função logo abaixo
@app.route("/")
#Função -> O que vc quer exibir naquela pagina
def homepage():
    return render_template("homepage.html")

@app.route("/Resultados", methods=["POST"])
def resultados():
    numerodurh = request.form['text']
    processed_text = numerodurh.upper()
    return render_template("homepage.html", msg=processed_text)

#Colocar o site no ar
if __name__ == "__main__":
    app.run(debug=True)