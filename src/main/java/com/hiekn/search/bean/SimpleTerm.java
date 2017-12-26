package com.hiekn.search.bean;

public class SimpleTerm{

    private String word;
    private String nature;
    private boolean newWord = false;

    public SimpleTerm(String word, String nature) {
        this.word = word;
        this.nature = nature;
    }

    public boolean isNewWord() {
        return newWord;
    }

    public void setNewWord(boolean newWord) {
        this.newWord = newWord;
    }

    public String getNature() {
        return nature;
    }

    public void setNature(String nature) {
        this.nature = nature;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

}