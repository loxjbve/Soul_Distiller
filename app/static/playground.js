const history = document.getElementById("chat-history");
const input = document.getElementById("playground-input");
const form = document.getElementById("playground-form");

if (history && input && form) {
    history.scrollTop = history.scrollHeight;
    input.addEventListener("keydown", (event) => {
        if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            form.requestSubmit();
        }
    });
}
