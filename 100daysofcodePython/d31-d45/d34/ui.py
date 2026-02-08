from tkinter import *
from quiz_brain import QuizBrain

THEME_COLOR = "#375362"
true_path = '100daysofcodePython/d31-d45/d34/images/true.png'
false_path = '100daysofcodePython/d31-d45/d34/images/false.png'

class QuizInterface:
    def __init__(self, quiz_brain: QuizBrain):
        self.window = Tk()
        self.window.title('Quizzler')
        self.window.configure(bg=THEME_COLOR, padx=20, pady=20)
        self.window.tk_setPalette(background=THEME_COLOR)
        self.quiz = quiz_brain
        self.feedback_timer = None
        
        self.score_label = Label(text='Score: 0', fg='white')
        self.score_label.grid(row=0, column=1)
        
        self.canvas = Canvas(width=300, height=250, bg='white')
        self.question_text = self.canvas.create_text(150, 125, 
                    text='', fill=THEME_COLOR, font=('Ariel', 20, 'italic'), width=280)
        self.canvas.grid(row=1, column=0, columnspan=2, pady=50)
        
        true_image = PhotoImage(file=true_path)
        self.true_button = Button(image=true_image, width=100, height=97, command=self.guess_true)
        self.true_button.grid(row=2, column=0)
        
        false_image = PhotoImage(file=false_path)
        self.false_button = Button(image=false_image, width=100, height=97, command=self.guess_false)
        self.false_button.grid(row=2, column=1)
        
        self.get_next_question()
        
        self.window.mainloop()
        
    def get_next_question(self):
        self.canvas.configure(bg='white')
        if self.quiz.still_has_questions():
            self.score_label.config(text=f'Score: {self.quiz.score}')
            q_text = self.quiz.next_question()
            self.canvas.itemconfig(self.question_text, text=q_text)
        else:
            self.canvas.itemconfig(self.question_text, text='Quiz Over!') 
            self.true_button.config(state='disabled')  
            self.false_button.config(state='disabled')  
        
        
    def guess_true(self):
        is_right = self.quiz.check_answer('True')
        self.give_feedback(is_right)

    def guess_false(self):
        is_right = self.quiz.check_answer('False')
        self.give_feedback(is_right)
    
    def give_feedback(self, is_right):
        if is_right:
            self.canvas.configure(bg='green')
        else:
            self.canvas.configure(bg='red')
        self.feedback_timer = self.window.after(1000, self.get_next_question)