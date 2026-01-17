from question_model import Question
from data import question_data
from quiz_brain import QuizBrain

question_bank = []
for question in question_data:
    new_question = Question(question['text'], question['answer'])
    question_bank.append(new_question)

# print(question_bank)

quiz1 = QuizBrain(question_bank)

while quiz1.still_has_questions():
    quiz1.next_question()

print('You have completed the quiz!')

print(f'Your final score was: {quiz1.score}/{quiz1.question_number}')